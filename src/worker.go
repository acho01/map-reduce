package main

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

	//create intermediate files
	for i := 0; i < current_worker.reduceAllocLen; i++ {
		f, _ := os.Create(fmt.Sprintf("mr-%v-%v", current_worker.id, i))
		f.Close()
	}

	f, _ := os.Create(current_worker.out_file)
	f.Close()

	for {
		reply := get_task()
		if reply.File != "" {
			if reply.Ttype == MAP {
				kv_list := mapf(reply.File, *map_read(&reply.File))
				for _, kv := range kv_list {
					map_write(&kv)
				}
			} else {

			}
			task_complete_handler(reply.Ttype)
		}
	}
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

func get_task() GetTaskReply {
	args := GetTaskArgs{current_worker.id}
	reply := GetTaskReply{}
	rpc_call("Controller.GetTask", &args, &reply)
	return reply
}

func task_complete_handler(ttype int) {
	args := TaskCompleteArgs{}
	args.Id = current_worker.id
	args.Ttype = ttype
	reply := PlainReply{}
	rpc_call("Controller.TaskComplete", &args, &reply)
}

func map_read(file *string) *string {
	ofile, _ := os.Open(*file)
	defer ofile.Close()
	b, _ := ioutil.ReadAll(ofile)
	content := string(b)
	return &content
}

func map_write(kv *KeyValue) {
	k := ihash(kv.Key) % current_worker.reduceAllocLen
	b, _ := json.Marshal(&kv)
	midfile := fmt.Sprintf("mr-%v-%v", current_worker.id, k)
	f, _ := os.OpenFile(midfile, os.O_APPEND|os.O_WRONLY, 0666)
	f.WriteString(string(b) + "\n")
	f.Close()
}

func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func rpc_call(rpcname string, args interface{}, reply interface{}) bool {
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
