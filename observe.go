package gosnowflake

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/mailru/easyjson"
	_ "github.com/mailru/easyjson/gen" // This is required to have go mod vendor not remove the package
	"github.com/mailru/easyjson/jlexer"
)

const (
	// ResponseBodyLimit limits http response to be 100MB to avoid overwhelming the scheduler
	ResponseBodyLimit = 100 * 1024 * 1024
)

// ErrResponseTooLarge means the reponse is too large (thanks linter for these useful comments!)
var ErrResponseTooLarge = fmt.Errorf("response is too large")

func decodeResponse(body io.ReadCloser, resp interface{}) error {
	lr := io.LimitReader(body, ResponseBodyLimit)
	var err error
	if v, is := resp.(easyjson.Unmarshaler); is {
		err = easyjson.UnmarshalFromReader(lr, v)
	} else {
		err = json.NewDecoder(lr).Decode(resp)
	}
	var lexerErr *jlexer.LexerError
	if err != nil &&
		(errors.Is(err, io.ErrUnexpectedEOF) ||
			(errors.As(err, &lexerErr) && (lr.(*io.LimitedReader).N <= 0))) {
		return ErrResponseTooLarge
	}
	return err
}
