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

type TaskState int

const (
	Idle TaskState = iota
	InProgress 
	Completed
)

type TaskInfo struct {
	State TaskState
	StartTime time.Time
}

type Phase int

const (
	MapPhase Phase = iota
	ReducePhase
	DonePhase
)	

type Coordinator struct {
	files []string 			// input files
	nReduce int				// number of reduce tasks
	mapTasks []TaskInfo 	// map tasks info
	reduceTasks []TaskInfo 	// reduce tasks info
	phase Phase		  		// current phase
	mu sync.Mutex
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
    c.mu.Lock()
    defer c.mu.Unlock()

    switch c.phase {

    //-------------------------------------------------------------------
    //                         MAP PHASE
    //-------------------------------------------------------------------
    case MapPhase:
        // 1. proiritize to assign Idle map tasks
        for i, t := range c.mapTasks {
            if t.State == Idle {
                reply.TaskType = TaskMap
                reply.FileName = c.files[i]
                reply.MapTaskID = i
                reply.NReduce = c.nReduce
                c.mapTasks[i].State = InProgress
                c.mapTasks[i].StartTime = time.Now()
                return nil
            }
        }

        // 2. retry InProgress map tasks that have timed out
        for i, t := range c.mapTasks {
            if t.State == InProgress &&
                time.Since(t.StartTime) > 10*time.Second {

                // reassign this map task
                reply.TaskType = TaskMap
                reply.FileName = c.files[i]
                reply.MapTaskID = i
                reply.NReduce = c.nReduce
                c.mapTasks[i].StartTime = time.Now()
                return nil
            }
        }

        // 3. check if all map tasks are completed
        allComplete := true
        for _, t := range c.mapTasks {
            if t.State != Completed {
                allComplete = false
                break
            }
        }

        if allComplete {
            c.phase = ReducePhase
            reply.TaskType = TaskNone
            return nil
        }

        reply.TaskType = TaskNone
        return nil

    //-------------------------------------------------------------------
    //                       REDUCE PHASE
    //-------------------------------------------------------------------
    case ReducePhase:
        // 1. proiritize to assign Idle reduce tasks
        for i, t := range c.reduceTasks {
            if t.State == Idle {
                reply.TaskType = TaskReduce
                reply.ReduceTaskID = i
                reply.NReduce = c.nReduce

                c.reduceTasks[i].State = InProgress
                c.reduceTasks[i].StartTime = time.Now()
                return nil
            }
        }

        // 2. retry InProgress reduce tasks that have timed out
        for i, t := range c.reduceTasks {
            if t.State == InProgress &&
                time.Since(t.StartTime) > 10*time.Second {

                reply.TaskType = TaskReduce
                reply.ReduceTaskID = i
                reply.NReduce = c.nReduce

                c.reduceTasks[i].StartTime = time.Now()
                return nil
            }
        }

        // 3. check if all reduce tasks are completed
        allComplete := true
        for _, t := range c.reduceTasks {
            if t.State != Completed {
                allComplete = false
                break
            }
        }

        if allComplete {
            c.phase = DonePhase
            reply.TaskType = TaskExit
            return nil
        }

        reply.TaskType = TaskNone
        return nil

    //-------------------------------------------------------------------
    //                           DONE PHASE
    //-------------------------------------------------------------------
    case DonePhase:
        reply.TaskType = TaskExit
        return nil
    }

    return nil
}

func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	switch args.TaskType {
	case TaskMap:
		if c.mapTasks[args.MapTaskID].State == InProgress {
			c.mapTasks[args.MapTaskID].State = Completed
		}
	case TaskReduce:
		if c.reduceTasks[args.ReduceTaskID].State == InProgress {
			c.reduceTasks[args.ReduceTaskID].State = Completed
		}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}
//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {

	// Your code here.
	c := &Coordinator{}

	c.files = files
	c.nReduce = nReduce
	c.phase = MapPhase
	
	c.mapTasks = make([]TaskInfo, len(files))
	c.reduceTasks = make([]TaskInfo, nReduce)
	go c.server()
	return c
}
