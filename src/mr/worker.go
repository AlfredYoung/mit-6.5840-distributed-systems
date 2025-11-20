package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	// "strconv"
	"time"
	"path/filepath"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key
type ByKey []KeyValue

func (a ByKey) Len() int {return len(a)}
func (a ByKey) Swap(i, j int) {a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool {return a[i].Key < a[j].Key}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}

		ok := call("Coordinator.GetTask", &args, &reply)
		// if coordinator has exited
		if !ok {
			return 
		}

		switch reply.TaskType {
		case TaskMap:
			if err := doMapTask(&reply, mapf); err == nil {
				reportMapDone(reply.MapTaskID)
			}
		case TaskReduce:
			 if err := doReduceTask(&reply, reducef); err == nil {
				reportReduceDone(reply.ReduceTaskID)
			}
		case TaskNone:
			time.Sleep(time.Millisecond * 100)
		case TaskExit:
			return
		}
	}
}

// doMapTask
func doMapTask(task *GetTaskReply, mapf func(string, string) []KeyValue) error {
	// read input file
	filename := task.FileName
	content, err := os.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("cannot read %v: %v", filename, err)
	}
	// call mapf
	kva := mapf(filename, string(content))

	// partition kva into nReduce intermediate files
	nReduce := task.NReduce
	buckets := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		r := ihash(kv.Key) % nReduce
		buckets[r] = append(buckets[r], kv)
	}

	// write intermediate files
    for r, kvs := range buckets {
        intermediateFile := fmt.Sprintf("mr-%d-%d", task.MapTaskID, r)
        
        // 1. create temporary file
        tmpFile, err := os.CreateTemp(".", "mr-tmp-*")
        if err != nil {
            return fmt.Errorf("cannot create temp file: %v", err)
        }

        // 2. write to temporary file
        enc := json.NewEncoder(tmpFile)
        for _, kv := range kvs {
            if err := enc.Encode(&kv); err != nil {
                tmpFile.Close()
                os.Remove(tmpFile.Name()) // write error, remove temp file
                return fmt.Errorf("cannot encode kv: %v", err)
            }
        }
        
        // close temporary file
        tmpFile.Close()

        // 3. atomic rename to final intermediate file
		// if the rename fails, remove the temp file
		// if two workers try to do the rename at the same time, only one will succeed
        if err := os.Rename(tmpFile.Name(), intermediateFile); err != nil {
            os.Remove(tmpFile.Name())
            return fmt.Errorf("cannot rename file: %v", err)
        }
    }
	return nil
}

// doReduceTask
func doReduceTask(task *GetTaskReply, reducef func(string, []string) string) error {
	reduceID := task.ReduceTaskID
	
	// refer to mrsequential.go for reduce logic
	// read intermediate files named "mr-X-reduceID"
	intermediate := []KeyValue{}
	files, _ := filepath.Glob(fmt.Sprintf("mr-*-%d", reduceID))
	for _, fname := range files {
		f, err := os.Open(fname)
		if err != nil {
			return err
		}

		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err = dec.Decode(&kv); err != nil {
				if err == io.EOF {
					break
				}
				break 
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))
	
	oname := fmt.Sprintf("mr-out-%d", reduceID)
    // 1. create temporary file
    tmpFile, err := os.CreateTemp(".", "mr-out-tmp-*")
    if err != nil {
        return err
    }

    i := 0
    for i < len(intermediate) {
        j := i + 1
        for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
            j++
        }
        values := []string{}
        for k := i; k < j; k++ {
            values = append(values, intermediate[k].Value)
        }

        output := reducef(intermediate[i].Key, values)
        
        // 2. write to temporary file
        if _, err := fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output); err != nil {
             tmpFile.Close()
             os.Remove(tmpFile.Name())
             return err
        }
        i = j
    }
    
    tmpFile.Close()

    // 3. atomic rename to final output file
    if err := os.Rename(tmpFile.Name(), oname); err != nil {
        os.Remove(tmpFile.Name())
        return err
    }
	return nil
}

func reportMapDone(mapTaskID int) {
	args := ReportTaskArgs {
		TaskType: TaskMap,
		MapTaskID: mapTaskID,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return  
	}
}

func reportReduceDone(reduceTaskID int) {
	args := ReportTaskArgs {
		TaskType: TaskReduce,
		ReduceTaskID: reduceTaskID,
	}
	reply := ReportTaskReply{}
	ok := call("Coordinator.ReportTask", &args, &reply)
	if !ok {
		return  
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}


//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
