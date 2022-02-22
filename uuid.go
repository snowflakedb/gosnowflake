// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/rand"
	"fmt"
)

const rfc4122 = 0x40

type uuid [16]byte

var nilUUID uuid

func newUUID() uuid {
	var u uuid
	rand.Read(u[:])
	u[8] = (u[8] | rfc4122) & 0x7F

	var version byte = 4
	u[6] = (u[6] & 0xF) | (version << 4)
	return u
}

func (u uuid) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}
