package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"
)

// Master FileMap: filename -> unstarted:0 / working:1
type Master struct {
	// Your definitions here.
	mu sync.Mutex
	MapperTask map[string]int
	MapFinished int
	ReducerTask map[int]int
	ReduceFinished int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
// serialID used to count rpc call
var serialID int = 10
//
func (m *Master) TaskAssign(args *TaskAssignArgs, reply *TaskAssignReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.MapFinished != 10 {
		//check undone task and do map
		for filename, status := range m.MapperTask {
			if status == 0 {
				// No.serialID rpc is responsible for handling this task
				status = serialID
				reply.RPCID = serialID
				reply.TaskNo = m.MapFinished
				reply.Filename = filename
				reply.TaskType = "map"
				serialID ++
				go func() {
					time.Sleep(10 * time.Second)
					m.mu.Lock()
					defer m.mu.Unlock()
					// after 10s task undone, cancel to give others
					if m.MapperTask[filename] > 2  {
						m.MapperTask[filename] = 0
					}
				}()
			}
		}
	} else if m.ReduceFinished != 10 {
		// check undone task and do reduce
		for tasknum, status := range m.ReducerTask {
			if status == 0 {
				status = serialID
				reply.RPCID = serialID
				reply.TaskNo = tasknum
				reply.TaskType = "reduce"
				serialID ++
				go func() {
					time.Sleep(10)
					m.mu.Lock()
					defer m.mu.Unlock()
					if m.ReducerTask[tasknum] > 2 {
						m.ReducerTask[tasknum] = 0
					}
				}()
			}
		}
	} else {
		// already done, go exit
		reply.TaskType = "none"
	}
	return nil
}
//
func (m *Master) MapFinish(args *MapFinishedArgs, reply *MapFinishedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.MapperTask[args.MapFilename] == args.RPCID {
		m.MapperTask[args.MapFilename] = 2
		m.MapFinished ++
	}
	return nil
}
//
func (m *Master) ReduceFinish(args *ReduceFinishedArgs, reply *ReduceFinishedReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.ReducerTask[args.ReduceTaskNo] == args.RPCID {
		m.ReducerTask[args.ReduceTaskNo] = 2
		m.ReduceFinished ++
	}
	
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	listener, err := net.Listen("tcp", ":21234")
	//sockname := masterSock()
	//os.Remove(sockname)
	//l, e := net.Listen("unix", sockname)
	if err != nil {
		log.Fatal("listen error:", err)
	}
	go http.Serve(listener, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false
	// Your code here.
	if m.ReduceFinished == 10 {
		ret = true
	}
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := new(Master)

	// Your code here.
	// initialize master data structure
	m.MapperTask = make(map[string]int)
	m.ReducerTask = make(map[int]int)
	for _, filename := range files {
		m.MapperTask[filename] = 0
	}
	for i := 0; i < nReduce; i++ {
		m.ReducerTask[i] = 0
	}
	m.MapFinished, m.ReduceFinished = 0, 0

	m.server()
	return m
}
