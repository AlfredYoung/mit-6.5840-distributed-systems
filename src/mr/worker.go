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
	"strconv"
	"time"
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
		replyMsg := CallForTask()

		switch replyMsg.RplType {
		case RplMapTaskAlloc:
			err := HandleMapTask(replyMsg, mapf)
			if err == nil {
				_ = CallForReportStatus(MapSuccess, replyMsg.TaskId)
			} else {
				_ = CallForReportStatus(MapFailed, replyMsg.TaskId)
			}

		case RplReduceTaskAlloc:
			err := HandleReduceTask(replyMsg, reducef)
			if err == nil {
				_ = CallForReportStatus(ReduceSuccess, replyMsg.TaskId)
			} else {
				_ = CallForReportStatus(ReduceFailed, replyMsg.TaskId)
			}

		case RplWait:
			time.Sleep(10 * time.Second)

		case RplShutdown:
			os.Exit(0)
		}

		time.Sleep(time.Second)
	}
}

// HandleMapTask executes a map task and generates intermediate files.
func HandleMapTask(reply *MessageReply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.TaskName)
	if err != nil {
		return err
	}
	defer file.Close()

	content, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	kva := mapf(reply.TaskName, string(content))
	sort.Sort(ByKey(kva))

	tempFiles := make([]*os.File, reply.NReduce)
	encoders := make([]*json.Encoder, reply.NReduce)

	for _, kv := range kva {
		redID := ihash(kv.Key) % reply.NReduce
		if encoders[redID] == nil {
			tempFile, err := os.CreateTemp("", fmt.Sprintf("mr-map-tmp-%d", redID))
			if err != nil {
				return err
			}
			defer tempFile.Close()
			tempFiles[redID] = tempFile
			encoders[redID] = json.NewEncoder(tempFile)
		}
		if err := encoders[redID].Encode(&kv); err != nil {
			return err
		}
	}

	for i, f := range tempFiles {
		if f != nil {
			fileName := f.Name()
			f.Close()
			newName := fmt.Sprintf("mr-out-%d-%d", reply.TaskId, i)
			if err := os.Rename(fileName, newName); err != nil {
				return err
			}
		}
	}

	return nil
}

// HandleReduceTask executes a reduce task by reading intermediate files.
func HandleReduceTask(reply *MessageReply, reducef func(string, []string) string) error {
	keyID := reply.TaskId
	kvs := map[string][]string{}

	fileList, err := ReadSpecificFile(keyID, "./")
	if err != nil {
		return err
	}

	for _, file := range fileList {
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
		file.Close()
	}

	var keys []string
	for key := range kvs {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	oname := "mr-out-" + strconv.Itoa(keyID)
	ofile, err := os.Create(oname)
	if err != nil {
		return err
	}
	defer ofile.Close()

	for _, key := range keys {
		output := reducef(key, kvs[key])
		if _, err := fmt.Fprintf(ofile, "%v %v\n", key, output); err != nil {
			return err
		}
	}

	DelFileByReduceId(keyID, "./")
	return nil
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

// CallForReportStatus sends task result (success/failure) to the coordinator.
func CallForReportStatus(result TaskResult, taskID int) bool {
	args := MessageSend{
		ReqType: ReqReportTask,
		TaskId:  taskID,
		Result:  result,
	}

	return call("Coordinator.NoticeResult", &args, nil)
}

// CallForTask requests a new task from the coordinator.
func CallForTask() *MessageReply {
	args := MessageSend{
		ReqType: ReqAskForTask,
	}
	reply := MessageReply{}

	if call("Coordinator.AskForTask", &args, &reply) {
		return &reply
	}
	return nil
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
