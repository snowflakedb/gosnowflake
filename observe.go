package gosnowflake

import (
	"encoding/json"
	"fmt"
	"io"
)

const (
	// limit http response to be 100MB to avoid overwhelming the scheduler
	ResponseBodyLimit = 100 * 1024 * 1024
)

var ErrResponseTooLarge = fmt.Errorf("response is too large")

type limitedJsonDecoder struct {
	decoder *json.Decoder
}

func (d *limitedJsonDecoder) Decode(v interface{}) error {
	err := d.decoder.Decode(v)
	if err == io.ErrUnexpectedEOF {
		return ErrResponseTooLarge
	} else {
		return err
	}
}

func newLimitedJsonDecoder(buf io.ReadCloser) *limitedJsonDecoder {
	return &limitedJsonDecoder{
		decoder: json.NewDecoder(io.LimitReader(buf, ResponseBodyLimit)),
	}
}
