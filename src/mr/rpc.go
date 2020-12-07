package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//编写一个master的RPC服务器，来接受worker的访问
//对于worker的数据使用回调的方式将数据传输回去
//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	NOWORK Status = 0
	MAP    Status = 1
	REDUCE Status = 2
)
const(
	MAXTIMES int = 30
)

type RequestAckArgs struct{	
	jobType Status
	filepath string
	nMap int 
	nReduce int 
}

type RequestJobReply struct{
	jobType Status
	fileName string
	nreduce int 
	nmap int 
}
// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
