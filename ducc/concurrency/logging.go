package concurrency

import (
	"bytes"
	"fmt"
	"log"
	"sync"

	"github.com/google/uuid"
)

type TaskStatus int

const (
	TS_NotStarted TaskStatus = iota
	TS_Running
	TS_Success
	TS_Failure
	TS_Aborted
)

var localHandles []*StatusHandle

type StatusHandle struct {
	Id             uuid.UUID
	OperationType  string
	Name           string
	StatusMutex    *sync.Mutex
	Status         TaskStatus
	Logger         LoggerWithBuffer
	RelationsMutex *sync.Mutex
	ParentHandle   *StatusHandle
	ChildHandles   []*StatusHandle
}

func NewStatusHandle(status TaskStatus) *StatusHandle {
	id, _ := uuid.NewRandom()
	sh := &StatusHandle{
		Id:             id,
		StatusMutex:    &sync.Mutex{},
		Status:         status,
		Logger:         NewLoggerWithBuffer(),
		RelationsMutex: &sync.Mutex{},
		ChildHandles:   []*StatusHandle{},
	}
	return sh
}

func (sh *StatusHandle) SetStatus(status TaskStatus) TaskStatus {
	// If the status is already set to a final state, don't change it
	if sh.Status == TS_Success || sh.Status == TS_Failure || sh.Status == TS_Aborted {
		return sh.Status
	}

	sh.StatusMutex.Lock()
	defer sh.StatusMutex.Unlock()
	sh.Status = status

	return status
}

func (sh *StatusHandle) addChildHandle(child *StatusHandle) {
	sh.RelationsMutex.Lock()
	defer sh.RelationsMutex.Unlock()
	sh.ChildHandles = append(sh.ChildHandles, child)
}

func (sh *StatusHandle) printStatus() {
	sh.StatusMutex.Lock()
	defer sh.StatusMutex.Unlock()
	fmt.Printf("Status of %s: %d\n", sh.Id.String(), sh.Status)
}

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
