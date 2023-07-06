package concurrency

import (
	"bytes"
	"log"
	"strings"
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

func (h *PacketHandle) GetAllLogs(buffer *bytes.Buffer, indentLevel int) {
	// Write the logs for this event, indented by indentLevel
	indents := string(bytes.Repeat([]byte(" "), indentLevel))
	text := strings.Trim(h.Log.logBuffer.String(), "\n\t ")
	if len(text) > 0 {
		lines := bytes.Split([]byte(text), []byte("\n"))
		for i, line := range lines {
			lines[i] = append([]byte(indents), line...)
		}
		buffer.Write(bytes.Join(lines, []byte("\n")))
		buffer.Write([]byte("\n"))
	}

	// Write the logs for all children, indented by indentLevel+1
	h.childrenMutex.Lock()
	for i, child := range h.children {
		buffer.WriteString(indents + " " + child.name + ":\n")
		child.GetAllLogs(buffer, indentLevel+1)
		if i < len(h.children)-1 {
			buffer.WriteString("\n")
		}
	}
	h.childrenMutex.Unlock()
}
