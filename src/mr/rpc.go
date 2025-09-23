package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "errors"

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
type MsgType int

var (
	BadMsgType = errors.New("Bad message type!")
	NoMoreTask = errors.New("No more task left!")
)


const (
	AskForTask 	MsgType = iota
	MapTaskAlloc
	ReduceTaskAlloc
	MapSuccess
	MapFailed
	ReduceSuccess
	ReduceFailed
	Shutdown
	Wait
)

type MessageSend struct {
	MsgType MsgType
	TaskId int
}

type MessageReply struct {
	Msgtype MsgType
	TaskId int
	NReduce int
	TaskName string
}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
