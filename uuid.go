// Copyright (c) 2022 Snowflake Computing Inc. All rights reserved.

package gosnowflake

import (
	"crypto/rand"
	"database/sql/driver"
	"fmt"
	"strconv"
)

const rfc4122 = 0x40

// UUID is a RFC4122 compliant uuid type
type UUID [16]byte

var nilUUID UUID

// NewUUID creates a new snowflake UUID
func NewUUID() UUID {
	var u UUID
	_, err := rand.Read(u[:])
	if err != nil {
		logger.Warnf("error while reading random bytes to UUID. %v", err)
	}
	u[8] = (u[8] | rfc4122) & 0x7F

	var version byte = 4
	u[6] = (u[6] & 0xF) | (version << 4)
	return u
}

func getChar(str string) byte {
	i, _ := strconv.ParseUint(str, 16, 8)
	return byte(i)
}

// ParseUUID parses a string of xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx into its UUID form
func ParseUUID(str string) UUID {
	return UUID{
		getChar(str[0:2]), getChar(str[2:4]), getChar(str[4:6]), getChar(str[6:8]),
		getChar(str[9:11]), getChar(str[11:13]),
		getChar(str[14:16]), getChar(str[16:18]),
		getChar(str[19:21]), getChar(str[21:23]),
		getChar(str[24:26]), getChar(str[26:28]), getChar(str[28:30]), getChar(str[30:32]), getChar(str[32:34]), getChar(str[34:36]),
	}
}

func (u UUID) String() string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", u[0:4], u[4:6], u[6:8], u[8:10], u[10:])
}

// This is for unit testing scans/value of UUIDs being inserted/read to/from the DB - not intended for public use
type testUUID = UUID

func newTestUUID() testUUID {
	return testUUID(NewUUID())
}

func parseTestUUID(str string) testUUID {
	return testUUID(ParseUUID(str))
}

// Scan implements sql.Scanner so UUIDs can be read from databases transparently.
// Currently, database types that map to string and []byte are supported. Please
// consult database-specific driver documentation for matching types.
func (uuid *testUUID) Scan(src interface{}) error {
	switch src := src.(type) {
	case nil:
		return nil

	case string:
		// if an empty UUID comes from a table, we return a null UUID
		if src == "" {
			return nil
		}

		// see Parse for required string format
		u := ParseUUID(src)

		*uuid = testUUID(u)

	case []byte:
		// if an empty UUID comes from a table, we return a null UUID
		if len(src) == 0 {
			return nil
		}

		// assumes a simple slice of bytes if 16 bytes
		// otherwise attempts to parse
		if len(src) != 16 {
			return uuid.Scan(string(src))
		}
		copy((*uuid)[:], src)

	default:
		return fmt.Errorf("Scan: unable to scan type %T into UUID", src)
	}

	return nil
}

// Value implements sql.Valuer so that UUIDs can be written to databases
// transparently. Currently, UUIDs map to strings. Please consult
// database-specific driver documentation for matching types.
func (uuid testUUID) Value() (driver.Value, error) {
	return uuid.String(), nil
}
