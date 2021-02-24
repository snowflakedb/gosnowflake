// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"bytes"
	"context"
	"database/sql/driver"
	"reflect"
	"strconv"
	"strings"
)

//lint:file-ignore U1000 Ignore all unused code

const (
	stageName       = "SYSTEM$BIND"
	createStageStmt = "CREATE TEMPORARY STAGE " + stageName + " file_format=" +
		"(type=csv field_optionally_enclosed_by='\"')"

	inputStreamBufferSize      = 1024 * 1024 * 10
	maxBindingParamsForLogging = 1000
)

type bindUploader struct {
	ctx            context.Context
	sc             *snowflakeConn
	stagePath      string
	closed         bool
	fileCount      int
	arrayBindStage string
	bindings       map[string]execBindParameter
}

func (bu *bindUploader) upload(bindings []driver.NamedValue) (*execResponseData, error) {
	if !bu.closed {
		bindingRows, _ := bu.buildRowsAsBytes(bindings)
		startIdx := 0
		numBytes := 0
		rowNum := 0
		bu.fileCount = 0
		var data *execResponseData
		var err error
		for rowNum < len(bindingRows) {
			for numBytes < inputStreamBufferSize && rowNum < len(bindingRows) {
				numBytes += len(bindingRows[rowNum])
				rowNum++
			}
			// concatenate all byte arrays into 1 and put into input stream
			var b bytes.Buffer
			b.Grow(numBytes)
			for i := startIdx; i < rowNum; i++ {
				b.Write(bindingRows[i])
			}

			bu.fileCount++
			filename := strconv.Itoa(bu.fileCount)
			data, err = bu.uploadStreamInternal(&b, filename, true)
			if err != nil {
				return nil, err
			}
			startIdx = rowNum
			numBytes = 0
		}
		return data, nil
	}
	return nil, nil
}

func (bu *bindUploader) uploadStreamInternal(inputStream *bytes.Buffer, dstFilename string, compressData bool) (*execResponseData, error) {
	err := bu.createStageIfNeeded()
	if err != nil {
		return nil, err
	}
	stageName := bu.stagePath
	if stageName == "" {
		return nil, &SnowflakeError{
			Number:  ErrBindUpload,
			Message: "stage name is null",
		}
	}
	if dstFilename == "" {
		return nil, &SnowflakeError{
			Number:  ErrBindUpload,
			Message: "destination file is null",
		}
	}

	var putCommand strings.Builder
	// use a placeholder for source file
	putCommand.WriteString("put file:///tmp/placeholder ")
	// add stage name surrounded by quotations in case special chars are used in directory name
	putCommand.WriteString("'")
	putCommand.WriteString(stageName)
	putCommand.WriteString("'")
	putCommand.WriteString(" overwrite=true")
	data, err := bu.sc.exec(bu.ctx, putCommand.String(), false, false, false, []driver.NamedValue{})
	if err != nil {
		return nil, err
	}

	sfa := &snowflakeFileTransferAgent{
		data:                       data.Data,
		command:                    putCommand.String(),
		sourceStream:               inputStream,
		dstFileNameForStreamSource: dstFilename,
		compressSourceFromStream:   compressData,
	}
	sfa.execute()
	return sfa.result()
}

func (bu *bindUploader) createStageIfNeeded() error {
	if bu.arrayBindStage != "" {
		return nil
	}
	data, err := bu.sc.exec(bu.ctx, createStageStmt, false, false, false, []driver.NamedValue{})
	if !data.Success {
		code, err := strconv.Atoi(data.Code)
		if err != nil {
			return err
		}
		return &SnowflakeError{
			Number:   code,
			SQLState: data.Data.SQLState,
			Message:  err.Error(),
			QueryID:  data.Data.QueryID}
	}
	if err != nil {
		return err
	}
	bu.arrayBindStage = stageName
	if err != nil {
		newThreshold := "0"
		bu.sc.cfg.Params[sessionArrayBindStageThreshold] = &newThreshold
	}
	return nil
}

func (bu *bindUploader) getBindValues(bindings []driver.NamedValue) (map[string]execBindParameter, error) {
	if bu.bindings != nil {
		return bu.bindings, nil
	}
	tsmode := timestampNtzType
	idx := 1
	var err error
	bu.bindings = make(map[string]execBindParameter, len(bindings))
	for _, binding := range bindings {
		t := goTypeToSnowflake(binding.Value, tsmode)
		logger.WithContext(bu.ctx).Debugf("tmode: %v\n", t)
		if t == changeType {
			tsmode, err = dataTypeMode(binding.Value)
			if err != nil {
				return nil, err
			}
		} else {
			var val interface{}
			if t == sliceType {
				// retrieve array binding data
				t, val = snowflakeArrayToString(&binding)
			} else {
				val, err = valueToString(binding.Value, tsmode)
				if err != nil {
					return nil, err
				}
			}
			if t == nullType || t == unSupportedType {
				t = textType // if null or not supported, pass to GS as text
			}
			bu.bindings[strconv.Itoa(idx)] = execBindParameter{
				Type:  t.String(),
				Value: val,
			}
			idx++
		}
	}
	return bu.bindings, nil
}

// transpose the columns to rows and write them to a list of bytes
func (bu *bindUploader) buildRowsAsBytes(columns []driver.NamedValue) ([][]byte, error) {
	numColumns := len(columns)
	if columns[0].Value == nil {
		return nil, &SnowflakeError{
			Number:  ErrBindSerialization,
			Message: "no binds found in the first column",
		}
	}

	_, arr := snowflakeArrayToString(&columns[0])
	numRows := len(arr)
	for i := 0; i < numColumns; i++ {
		_, arr = snowflakeArrayToString(&columns[i])
		iNumRows := len(arr)
		if iNumRows != numRows {
			return nil, &SnowflakeError{
				Number:      ErrBindSerialization,
				Message:     errMsgBindColumnMismatch,
				MessageArgs: []interface{}{i, iNumRows, numRows},
			}
		}
	}

	csvRows := make([][]byte, 0)
	rows := make([][]string, 0)
	for rowIdx := 0; rowIdx < numRows; rowIdx++ {
		rows = append(rows, make([]string, numColumns))
	}
	for colIdx := 0; colIdx < numColumns; colIdx++ {
		_, column := snowflakeArrayToString(&columns[colIdx])
		for rowIdx := 0; rowIdx < numRows; rowIdx++ {
			rows[rowIdx][colIdx] = column[rowIdx] // length of column = number of rows
		}
	}
	for _, row := range rows {
		csvRows = append(csvRows, bu.createCSVRecord(row))
	}
	return csvRows, nil
}

func (bu *bindUploader) createCSVRecord(data []string) []byte {
	var b strings.Builder
	b.Grow(1024)
	for i := 0; i < len(data); i++ {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(escapeForCSV(data[i]))
	}
	b.WriteString("\n")
	return []byte(b.String())
}

func (bu *bindUploader) arrayBindValueCount(bindValues []driver.NamedValue) int {
	if !bu.isArrayBind(bindValues) {
		return 0
	}
	_, arr := snowflakeArrayToString(&bindValues[0])
	return len(bindValues) * len(arr)
}

func (bu *bindUploader) isArrayBind(bindings []driver.NamedValue) bool {
	if len(bindings) == 0 {
		return false
	}
	for _, binding := range bindings {
		if supported := supportedArrayBind(&binding); !supported {
			return false
		}
	}
	return true
}

func (bu *bindUploader) close() {
	if !bu.closed {
		bu.closed = true
	}
}

func supportedArrayBind(nv *driver.NamedValue) bool {
	switch reflect.TypeOf(nv.Value) {
	case reflect.TypeOf(&intArray{}), reflect.TypeOf(&int32Array{}),
		reflect.TypeOf(&int64Array{}), reflect.TypeOf(&float64Array{}),
		reflect.TypeOf(&float32Array{}), reflect.TypeOf(&boolArray{}),
		reflect.TypeOf(&stringArray{}), reflect.TypeOf(&byteArray{}):
		return true
	default:
		// TODO SNOW-292862 date, timestamp, time
		// TODO SNOW-176486 variant, object, array
		return false
	}
}
