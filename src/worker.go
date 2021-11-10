package main

import (
	"fmt"
	"log"
	"net/rpc"
)

type Current_Worker struct {
	id             int
	reduceAllocLen int
	out_file       string
}

var current_worker Current_Worker

func main() {
	Worker(Map, Reduce)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	register_worker()
}

func register_worker() {
	args := PlainArgs{}
	reply := WorkerInitReply{}
	rpc_call("Controller.RegisterWorker", &args, &reply)
	current_worker.id = reply.ID
	current_worker.reduceAllocLen = reply.ReduceAllocLen
	current_worker.out_file = fmt.Sprintf("mr-out-%v", reply.ID)
	fmt.Printf("Worker %s registered!\n", reply)
}

func rpc_call(rpcname string, args interface{}, reply interface{}) bool {
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
