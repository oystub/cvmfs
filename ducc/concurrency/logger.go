package concurrency

import (
	"bytes"
	"log"
)

type LoggerWithBuffer struct {
	logBuffer *bytes.Buffer
	logger    *log.Logger
}

func NewLoggerWithBuffer() LoggerWithBuffer {
	buffer := bytes.NewBuffer([]byte{})
	return LoggerWithBuffer{
		logBuffer: buffer,
		logger:    log.New(buffer, "", log.LstdFlags),
	}
}

func (l *LoggerWithBuffer) GetLogger() *log.Logger {
	return l.logger
}

func (l *LoggerWithBuffer) GetText() string {
	return l.logBuffer.String()
}
