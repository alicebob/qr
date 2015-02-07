package qr

import (
	"log"
)

// Logger is anything which can Printf.
type Logger interface {
	// Printf is used for all warnings.
	Printf(string, ...interface{})
}

// StdLog is a Logger, which uses `log`.
type StdLog struct{}

// Printf is used for all warnings.
func (StdLog) Printf(s string, args ...interface{}) {
	log.Printf(s, args...)
}
