//go:build cgo

package cgo

var _ = func() any {
	Enabled = true
	return nil
}()
