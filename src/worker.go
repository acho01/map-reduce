package main

import (
	"bufio"
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
				interData := reduce_read(&reply.File, current_worker.reduceAllocLen)
				for k, v := range *interData {
					res := reducef(k, v)
					reduce_write(&k, &res)
				}
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

func reduce_write(k *string, res *string) {
	f, _ := os.OpenFile(current_worker.out_file, os.O_APPEND|os.O_WRONLY, 0666)
	f.WriteString(fmt.Sprintf("%v %v\n", *k, *res))
	f.Close()
}

func reduce_read(rnum *string, nworker int) *map[string][]string {
	m := make(map[string][]string)
	for i := 0; i < nworker; i++ {
		file := fmt.Sprintf("mr-%v-%v", i, *rnum)
		ofile, _ := os.Open(file)
		scanner := bufio.NewScanner(ofile)
		var tmp KeyValue
		for scanner.Scan() {
			json.Unmarshal([]byte(scanner.Text()), &tmp)
			m[tmp.Key] = append(m[tmp.Key], tmp.Value)
		}
		ofile.Close()
	}
	return &m
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
