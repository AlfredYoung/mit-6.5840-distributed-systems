package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type taskStatus int

const (
	idle taskStatus = iota
	running
	finished
	failed
)

type MapTaskInfo struct {
	TaskId    int
	Status    taskStatus
	StartTime int64
}

type ReduceTaskInfo struct {
	Status    taskStatus
	StartTime int64
}

type Coordinator struct {
	NReduce       int
	MapTasks      map[string]*MapTaskInfo
	MapSuccess    bool
	ReduceTasks   []*ReduceTaskInfo
	ReduceSuccess bool
	muMap         sync.Mutex
	muReduce      sync.Mutex
}

func (c *Coordinator) initTasks(files []string) {
	for idx, filename := range files {
		c.MapTasks[filename] = &MapTaskInfo{
			TaskId: idx,
			Status: idle,
		}
	}

	for idx := range c.ReduceTasks {
		c.ReduceTasks[idx] = &ReduceTaskInfo{
			Status: idle,
		}
	}
}

// AskForTask is an RPC handler for workers requesting tasks.
func (c *Coordinator) AskForTask(send *MessageSend, reply *MessageReply) error {
	// 检查请求类型：期望是请求任务
	if send.ReqType != ReqAskForTask {
		return BadMsgType
	}

	if !c.MapSuccess {
		c.muMap.Lock()
		countMapSuccess := 0

		for filename, taskinfo := range c.MapTasks {
			alloc := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime-taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				countMapSuccess++
			}

			if alloc {
				reply.RplType = RplMapTaskAlloc
				reply.TaskName = filename
				reply.NReduce = c.NReduce
				reply.TaskId = taskinfo.TaskId

				taskinfo.StartTime = time.Now().Unix()
				taskinfo.Status = running

				c.muMap.Unlock()
				return nil
			}
		}

		c.muMap.Unlock()

		if countMapSuccess < len(c.MapTasks) {
			reply.RplType = RplWait
			return nil
		} else {
			c.MapSuccess = true
		}
	}

	if !c.ReduceSuccess {
		c.muReduce.Lock()
		countReduceSuccess := 0

		for idx, taskinfo := range c.ReduceTasks {
			alloc := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime-taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				countReduceSuccess++
			}

			if alloc {
				reply.RplType = RplReduceTaskAlloc
				reply.TaskId = idx

				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()

				c.muReduce.Unlock()
				return nil
			}
		}
		c.muReduce.Unlock()

		if countReduceSuccess < len(c.ReduceTasks) {
			reply.RplType = RplWait
			return nil
		} else {
			c.ReduceSuccess = true
		}
	}

	reply.RplType = RplShutdown
	return nil
}

// NoticeResult is an RPC handler for workers reporting results.
func (c *Coordinator) NoticeResult(send *MessageSend, reply *MessageReply) error {
	// 这里使用 send.Result（TaskResult）来判断是哪种上报结果
	switch send.Result {
	case MapSuccess:
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == send.TaskId {
				v.Status = finished
				c.muMap.Unlock()
				return nil
			}
		}
		c.muMap.Unlock()

	case ReduceSuccess:
		c.muReduce.Lock()
		c.ReduceTasks[send.TaskId].Status = finished
		c.muReduce.Unlock()
		return nil

	case MapFailed:
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == send.TaskId && v.Status == running {
				v.Status = failed
				c.muMap.Unlock()
				return nil
			}
			// 注意：该 Unlock 在原代码中位于循环体内（保持原逻辑）
			c.muMap.Unlock()
		}

	case ReduceFailed:
		c.muReduce.Lock()
		if c.ReduceTasks[send.TaskId].Status == running {
			c.ReduceTasks[send.TaskId].Status = failed
		}
		c.muReduce.Unlock()
		return nil
	}

	return nil
}


//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {

	// Your code here.
	for _, taskinfo := range c.MapTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	for _, taskinfo := range c.ReduceTasks {
		if taskinfo.Status != finished {
			return false
		}
	}

	time.Sleep(time.Second * 3)

	return true
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	c := Coordinator{
		NReduce:     nReduce,
		MapTasks:    make(map[string]*MapTaskInfo),
		ReduceTasks: make([]*ReduceTaskInfo, nReduce),
	}
	c.initTasks(files)

	c.server()
	return &c
}
