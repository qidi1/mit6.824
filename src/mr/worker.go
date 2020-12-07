package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

const PREFIX = "mr-"
const MRPREFIX = "mr-out-"

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
}
func MapWork(mapf func(string, string) []KeyValue, reply RequestJobReply) error {
	filename := reply.fileName
	bytes, err := ioutil.ReadFile(filename)
	nreduce := reply.nreduce
	nmap := reply.nmap
	if err != nil {
		return err
	}
	content := string(bytes)
	target := mapf(filename, content)
	encoders := make([]*json.Encoder, 0, nreduce)
	for i := 0; i < nreduce; i++ {
		name := getFileName(nmap, i)
		file, _ := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
		encoders = append(encoders, json.NewEncoder(file))
		defer file.Close()
	}
	for i := 0; i < len(target); i++ {
		index := ihash(target[i].Key) % nreduce
		encoders[index].Encode(target[i])
	}
	return nil
}
func Reduce(reducef func(string, []string) string, reply RequestJobReply) {
	nmap := reply.nmap
	nreduce := reply.nreduce
	encs := make([]json.Decoder, nmap, nmap)
	for m := 0; m < nmap; m++ {
		fileName := getFileName(m, nreduce)
		rfile, err := os.OpenFile(fileName, os.O_RDONLY, 0666)
		if err != nil {
			fmt.Printf("can't open file %s in reduceTask %d", fileName, nreduce)
			return
		}
		defer rfile.Close()
		encs[m] = *json.NewDecoder(rfile)
	}
	kvs := make(map[string][]string, 0)
	for i := 0; i < nmap; i++ {
		var kv *KeyValue
		for {
			err := encs[i].Decode(&kv)
			if err != nil {
				fmt.Printf("can't decode %s %d %d", getFileName(i, nreduce), i, nreduce)
				break
			}
			kvs[kv.Key] = append(kvs[kv.Key], kv.Value)
		}
	}
	var keys []string
	for k := range kvs {
		keys = append(keys, k)
	}
	sort.Strings(keys)

}
func getFileName(nmap int, nreduce int) string {
	str, _ := os.Getwd()
	filename := fmt.Sprintf("map-%d-%d", nmap, nreduce)
	return str + "/" + filename
}

//
// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
