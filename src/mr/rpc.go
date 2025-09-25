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
var (
	BadMsgType = errors.New("Bad message type!")
	NoMoreTask = errors.New("No more task left!")
)


type RequestType int
type ReplyType int
type TaskResult int

const (
    // 请求类型
    ReqAskForTask RequestType = iota
    ReqReportTask
)

const (
    // 回复类型
    RplMapTaskAlloc ReplyType = iota
    RplReduceTaskAlloc
    RplWait
    RplShutdown
)

const (
    // 任务结果
    MapSuccess TaskResult = iota
    MapFailed
    ReduceSuccess
    ReduceFailed
)

type MessageSend struct {
    ReqType   RequestType
    TaskId    int
    TaskName  string
    Result    TaskResult
    ErrMsg    string
}

type MessageReply struct {
    RplType   ReplyType
    TaskId    int
    TaskName  string // map 任务时带上文件名
    NReduce   int
    NMap      int    // 如果 reduce 需要知道 map 数量
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
