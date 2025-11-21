package lock

import (
	"6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	key string
	myID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
        ck: ck,
        key: l,
        myID: kvtest.RandValue(8), // ervey lock needs a unique ID
    }
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
    for {
        value, version, err := lk.ck.Get(lk.key)
        if err == rpc.ErrNoKey {
            e := lk.ck.Put(lk.key, lk.myID, 0)
			if e == rpc.OK {
				return
			} else if e == rpc.ErrMaybe || e == rpc.ErrVersion {
				// for ErrMaybe We don't know if we acquired the lock; have to check again
				continue
			}
        }
        if err == rpc.OK {
            if value == lk.myID {
                return
            }
            // If someone else holds the lock, DON'T CAS here.
            // Just continue waiting.
            if value != "" { 
                continue
            }
            
			e := lk.ck.Put(lk.key, lk.myID, version)
			if e == rpc.OK {
				return
			} else if e == rpc.ErrMaybe || e == rpc.ErrVersion {
				// for ErrMaybe We don't know if we acquired the lock; have to check again
				continue
			}
        }
    }
}


func (lk *Lock) Release() {
	// Your code here
	for {
		value, version, err := lk.ck.Get(lk.key)
		if err == rpc.ErrNoKey {
			// Lock is already released
			return
		}
		
		if err == rpc.OK {
			if value != lk.myID {
				// Lock is held by someone else or release by previous loop; cannot release
				return
			}

			e := lk.ck.Put(lk.key, "", version)

			if e == rpc.OK {
				return
			} else if e == rpc.ErrMaybe {
				// for ErrMaybe We don't know if we released the lock; have to check again
				continue
			} else {
				return // for ErrVersion, someone else acquired the lock; cannot release
			}
		}
	}
}
