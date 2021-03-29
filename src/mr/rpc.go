package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	MAP_TASK    = 0
	REDUCE_TASK = 1
	IDLE_TASK   = 2
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type TaskRequest struct {
	Pad string
}

type TaskResponse struct {
	FileName string
	TaskType int
	TaskId   int
	NReduce  int
}

type CommitRequest struct {
	TaskType int
	TaskId   int
}

type CommitResponse struct {
	Succeed bool
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
