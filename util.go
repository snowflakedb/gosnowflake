package gosnowflake

import "database/sql/driver"

// integer min
func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// integer min
func intMin64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// integer max
func intMax(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func toNamedValues(values []driver.Value) []driver.NamedValue {
	namedValues := make([]driver.NamedValue, len(values))
	for idx, value := range values {
		namedValues[idx] = driver.NamedValue{Name: "", Ordinal: idx + 1, Value: value}
	}
	return namedValues
}
