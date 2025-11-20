package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
// import "errors"

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

type TaskType int

const (
    TaskNone TaskType = iota    // waiting for task
    TaskMap                     
    TaskReduce                  
    TaskExit                    // all tasks are done
)

type GetTaskArgs struct {
    WorkerID int // get assigned by coordinator
}

type GetTaskReply struct {
    TaskType TaskType

    FileName string     // filename for Map task
    MapTaskID int       

    ReduceTaskID int    

    NReduce int         // Reduce task count
}


type ReportTaskArgs struct {
    TaskType TaskType
    MapTaskID int
    ReduceTaskID int
}

type ReportTaskReply struct{}



// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
