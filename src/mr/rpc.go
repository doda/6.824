package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"time"
)

type TaskType int
type StatusT int
type Phase int

const (
	MapTaskT TaskType = iota
	ReduceTaskT
)
const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)

const (
	Pending StatusT = iota
	InProgress
	Done
)

type Task struct {
	ID      int
	Type    TaskType
	FName   string
	Status  StatusT
	Started time.Time
	NReduce int
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskInstance Task
	Finished     bool
}

type SubmitTaskArgs struct {
	ID   int
	Type TaskType
}

type SubmitTaskReply struct {
	Finished bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
