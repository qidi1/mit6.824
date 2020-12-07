package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
)

//编写worker注册的rpc

type Master struct {
	// Your definitions here.
	workers      []string
	mu           sync.Mutex
	muFile       sync.Mutex
	files        []MapFile
	fileStatus   map[MapFile]int
	nReduce      int
	nMap         int
	stage        Status
	muTemp       sync.Mutex
	temp         []ReduceWork
	tempStatus   map[ReduceWork]int
	endMapJob    int
	endReduceJob int
}

//num 记录map任务第几个
//status 记录当前map任务的状态
type MapFile struct {
	num  int
	name string
}

//num 记录reduce任务第几个
//status 记录当前reduce任务的状态,当超过一定的调用会被放回重新被调用
type ReduceWork struct {
	num int
}
type Status int32

// Your code here -- RPC handlers for the worker to call.
func (m *Master) register(worker string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workers = append(m.workers, worker)
}
func (m *Master) getwork(reply *RequestJobReply) error {
	fmt.Printf("Start AskForJob")
	if m.stage == NOWORK {
		reply.jobType = NOWORK
	} else if m.stage == MAP {
		m.muFile.Lock()
		defer m.muFile.Unlock()
		for k, v := range m.fileStatus {
			if v < 0 {
				delete(m.fileStatus, k)
			} else if v > 30 {
				delete(m.fileStatus, k)
				m.files = append(m.files, k)
			} else {
				m.fileStatus[k] += 1
			}
		}
		if len(m.files) == 0 {
			reply.jobType = m.stage
			return nil
		}
		file := m.files[0]
		m.files = m.files[1:]
		reply.nreduce = m.nReduce
		reply.fileName = file.name
		reply.nmap = file.num
	} else if m.stage == REDUCE {
		m.muTemp.Lock()
		defer m.muTemp.Unlock()
		for k, v := range m.tempStatus {
			if v < 0 {
				delete(m.tempStatus, k)
			} else if v > 30 {
				delete(m.tempStatus, k)
				m.temp = append(m.temp, k)
			} else {
				m.tempStatus[k] += 1
			}
		}
		if len(m.temp) == 0 {
			reply.jobType = m.stage
			return nil
		}
		reduceWork := m.temp[0]
		m.temp = m.temp[1:]
		reply.nreduce = reduceWork.num
	}
	return nil
}
func (m *Master) Ack(args RequestAckArgs) error {
	if args.jobType == MAP {
		key := MapFile{
			name: args.filepath,
			num:  args.nMap,
		}
		delete(m.fileStatus, key)
		m.endMapJob++
		if m.endMapJob == m.nMap {
			m.stage = REDUCE
		}
	} else if args.jobType == REDUCE {
		key := ReduceWork{
			num: args.nReduce,
		}
		delete(m.tempStatus, key)
		m.endReduceJob++
		if m.endMapJob == m.nMap {
			m.stage = MAP
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.

	m.server()
	return &m
}
