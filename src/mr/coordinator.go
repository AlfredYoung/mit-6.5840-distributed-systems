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
	idle 	taskStatus = iota
	running
	finished
	failed
)

type MapTaskInfo struct {
	TaskId int
	Status taskStatus
	StartTime int64
}

type ReduceTaskInfo struct {
	Status taskStatus
	StartTime int64
}

type Coordinator struct {
	// Your definitions here.
	NReduce 		int
	MapTasks 		map[string]*MapTaskInfo
	MapSuccess 		bool
	ReduceTasks 	[]*ReduceTaskInfo
	ReduceSuccess 	bool
	muMap 			sync.Mutex
	muReuce 		sync.Mutex
}

func (c *Coordinator) initTasks (files []string) {
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

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AskForTask(send *MessageSend, reply *MessageReply) bool {
	if send.MsgType != AskForTask {
		return false
	}
	if !c.MapSuccess {
		c.muMap.Lock()

		count_map_success := 0
		for filename, taskinfo := range c.MapTasks {
			alloc := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime - taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				count_map_success ++
			}

			if alloc {
				reply.Msgtype = MapTaskAlloc
				reply.TaskName = filename
				reply.NReduce = c.NReduce
				reply.TaskId = taskinfo.TaskId

				taskinfo.StartTime = time.Now().Unix()
				taskinfo.Status = running
				c.muMap.Unlock()
				return true
			}
		}

		c.muMap.Unlock()

		if count_map_success < len(c.MapTasks) {
			reply.Msgtype = Wait
			return true
		} else {
			c.MapSuccess = true
		}
	}

	if !c.ReduceSuccess {
		c.muReuce.Lock()

		count_reduce_success := 0
		for idx, taskinfo := range c.ReduceTasks {
			alloc := false
			if taskinfo.Status == idle || taskinfo.Status == failed {
				alloc = true
			} else if taskinfo.Status == running {
				curTime := time.Now().Unix()
				if curTime - taskinfo.StartTime > 10 {
					taskinfo.StartTime = curTime
					alloc = true
				}
			} else {
				count_reduce_success++
			}

			if alloc {
				reply.Msgtype = ReduceTaskAlloc
				reply.TaskId = idx
				
				taskinfo.Status = running
				taskinfo.StartTime = time.Now().Unix()

				c.muReuce.Unlock()
				return true
			}
		}
		c.muReuce.Unlock()

		if count_reduce_success < len(c.ReduceTasks) {
			reply.Msgtype = Wait
			return true
		} else {
			c.ReduceSuccess = true
		}

	}

	reply.Msgtype = Shutdown
	return true
}

func (c *Coordinator) NoticeResult(send *MessageSend, reply *MessageReply) bool {
	if send.MsgType == MapSuccess {
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == send.TaskId {
				v.Status = finished
				c.muMap.Unlock()
				return true
			}
		}
		c.muMap.Unlock()
	} else if send.MsgType == ReduceSuccess {
		c.muReuce.Lock()
		c.ReduceTasks[send.TaskId].Status = finished
		c.muReuce.Unlock()
		return true
	} else if send.MsgType == MapFailed {
		c.muMap.Lock()
		for _, v := range c.MapTasks {
			if v.TaskId == send.TaskId && v.Status == running {
				v.Status = failed
				c.muMap.Unlock()
				return true
			}
			c.muMap.Unlock()
		}
	} else if send.MsgType == ReduceFailed {
		c.muReuce.Lock()
		if c.ReduceTasks[send.TaskId].Status == running {
			c.ReduceTasks[send.TaskId].Status = failed
		}
		c.muReuce.Unlock()
		return true
	}
	return true
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
