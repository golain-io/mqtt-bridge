package bridge

import "fmt"

type BridgeError struct {
	Op  string
	Err error
}

func (e *BridgeError) Error() string {
	return fmt.Sprintf("mqtt bridge %s: %v", e.Op, e.Err)
}

func (e *BridgeError) Unwrap() error {
	return e.Err
}
