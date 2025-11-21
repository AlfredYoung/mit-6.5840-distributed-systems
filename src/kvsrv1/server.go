package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}
type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	m map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.
	kv.m = make(map[string]ValueVersion)
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if vv, ok := kv.m[args.Key]; ok {
		reply.Value = vv.Value
		reply.Version = vv.Version
		reply.Err = rpc.OK
	} else {
		reply.Err = rpc.ErrNoKey
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	vv, ok := kv.m[args.Key]
	if !ok {
		// key doesn't exist
		if args.Version == 0 {
			// install new key
			kv.m[args.Key] = ValueVersion{Value: args.Value, Version: 1}
			reply.Err = rpc.OK
		} else {
			// version mismatch
			reply.Err = rpc.ErrNoKey
		}
	} else {
		// key exists
		if args.Version == vv.Version {
			// version match; update value and version
			kv.m[args.Key] = ValueVersion{Value: args.Value, Version: vv.Version + 1}
			reply.Err = rpc.OK
		} else {
			// version mismatch
			reply.Err = rpc.ErrVersion
		}
	}
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
