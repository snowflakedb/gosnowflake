package gosnowflake

import (
	"bytes"
	"runtime"
	"strconv"
	"sync"
)

type valueAwaiterType struct {
	lockKey lockKeyType
	working bool
	cond    *sync.Cond
	mu      sync.Mutex
	h       *valueAwaitHolderType
}

func newValueAwaiter(lockKey lockKeyType, h *valueAwaitHolderType) *valueAwaiterType {
	ret := &valueAwaiterType{
		lockKey: lockKey,
		h:       h,
	}
	ret.cond = sync.NewCond(&ret.mu)
	return ret
}

func awaitValue[T any](valueAwaiter *valueAwaiterType, runFunc func() (T, error), acceptFunc func(t T, err error) bool, defaultFactoryFunc func() T) (T, error) {
	println("AAA", goid(), "entered awaitValue")
	valueAwaiter.mu.Lock()
	value, err := runFunc()
	if acceptFunc(value, err) {
		println("AAA", goid(), "value was ready")
		valueAwaiter.mu.Unlock()
		return value, err
	}
	if !valueAwaiter.working {
		println("AAA", goid(), "start working")
		valueAwaiter.working = true
		valueAwaiter.mu.Unlock()
		// continue working only in this thread
		return defaultFactoryFunc(), nil
	}
	ret, err := runFunc()
	for !acceptFunc(ret, err) {
		println("AAA", goid(), "waiting for value")
		valueAwaiter.cond.Wait()
		println("AAA", goid(), "woke up")
		ret, err = runFunc()
		if !acceptFunc(ret, err) && !valueAwaiter.working {
			println("AAA", goid(), "start working after wait")
			valueAwaiter.working = true
			valueAwaiter.mu.Unlock()
			return defaultFactoryFunc(), nil
		}
	}
	println("AAA", goid(), "value was ready after wait")
	valueAwaiter.mu.Unlock()
	return ret, err
}

func (v *valueAwaiterType) done() {
	println("AAA", goid(), "done working")
	v.mu.Lock()
	defer v.mu.Unlock()
	v.working = false
	v.cond.Broadcast()
	v.h.remove(v)
}

func (v *valueAwaiterType) resumeOne() {
	println("AAA", goid(), "resuming one")
	v.mu.Lock()
	defer v.mu.Unlock()
	v.working = false
	v.cond.Signal()
}

type valueAwaitHolderType struct {
	mu      sync.Mutex
	holders map[string]*valueAwaiterType
}

var valueAwaitHolder = newValueAwaitHolder()

func newValueAwaitHolder() *valueAwaitHolderType {
	return &valueAwaitHolderType{
		holders: make(map[string]*valueAwaiterType),
	}
}

func (h *valueAwaitHolderType) get(lockKey lockKeyType) *valueAwaiterType {
	lockID := lockKey.lockID()
	h.mu.Lock()
	defer h.mu.Unlock()
	holder, ok := h.holders[lockID]
	if !ok {
		holder = newValueAwaiter(lockKey, h)
		h.holders[lockID] = holder
	}
	return holder
}

func (h *valueAwaitHolderType) remove(v *valueAwaiterType) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.holders, v.lockKey.lockID())
}

func goid() int {
	buf := make([]byte, 32)
	n := runtime.Stack(buf, false)
	buf = buf[:n]
	// goroutine 1 [running]: ...

	buf, ok := bytes.CutPrefix(buf, []byte("goroutine "))
	if !ok {
		panic("cut prefix")
	}

	i := bytes.IndexByte(buf, ' ')
	if i < 0 {
		panic("index byte")
	}

	atoi, err := strconv.Atoi(string(buf[:i]))
	if err != nil {
		panic(err)
	}
	return atoi
}
