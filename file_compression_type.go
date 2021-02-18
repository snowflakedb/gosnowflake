// Copyright (c) 2021 Snowflake Computing Inc. All right reserved.

package gosnowflake

import (
	"fmt"
	"strings"
)

type compressionType struct {
	name          string
	fileExtension string
	mimeType      string
	mimeSubtypes  []string
	isSupported   bool
}

var compressionTypes = map[string]compressionType{
	"GZIP": {
		"GZIP",
		".gz",
		"application",
		[]string{"gzip", "x-gzip"},
		true,
	},
	"DEFLATE": {
		"DEFLATE",
		".deflate",
		"application",
		[]string{"zlib", "deflate"},
		true,
	},
	"RAW_DEFLATE": {
		"RAW_DEFLATE",
		".raw_deflate",
		"application",
		[]string{"raw_deflate"},
		true,
	},
	"BZIP2": {
		"BZIP2",
		".bz2",
		"application",
		[]string{"bzip2", "x-bzip2", "x-bz2", "x-bzip", "bz2"},
		true,
	},
	"LZIP": {
		"LZIP",
		".lz",
		"application",
		[]string{"lzip", "x-lzip"},
		false,
	},
	"LZMA": {
		"LZMA",
		".lzma",
		"application",
		[]string{"lzma", "x-lzma"},
		false,
	},
	"LZO": {
		"LZO",
		".lzo",
		"application",
		[]string{"lzo", "x-lzo"},
		false,
	},
	"XZ": {
		"XZ",
		".xz",
		"application",
		[]string{"xz", "x-xz"},
		false,
	},
	"COMPRESS": {
		"COMPRESS",
		".Z",
		"application",
		[]string{"compress", "x-compress"},
		false,
	},
	"PARQUET": {
		"PARQUET",
		".parquet",
		"snowflake",
		[]string{"parquet"},
		true,
	},
	"ZSTD": {
		"ZSTD",
		".zst",
		"application",
		[]string{"zstd", "x-zstd"},
		true,
	},
	"BROTLI": {
		"BROTLI",
		".br",
		"application",
		[]string{"br", "x-br"},
		true,
	},
	"ORC": {
		"ORC",
		".orc",
		"snowflake",
		[]string{"orc"},
		true,
	},
}

type fileCompressionType struct {
	subTypeToMeta map[string]compressionType
}

func (fct *fileCompressionType) init() {
	fct.subTypeToMeta = make(map[string]compressionType)
	for _, meta := range compressionTypes {
		for _, subType := range meta.mimeSubtypes {
			fct.subTypeToMeta[subType] = meta
		}
	}
}

func (fct *fileCompressionType) lookupByMimeSubType(mimeSubType string) *compressionType {
	fmt.Println(mimeSubType)
	if val, ok := fct.subTypeToMeta[strings.ToLower(mimeSubType)]; ok {
		return &val
	}
	return new(compressionType)
}
