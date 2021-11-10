package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Task struct {
	workerId int
	status   int
	ttype    int
	file     string
}

type Workerr struct {
	id int
}

type Controller struct {
	tasks          []Task
	workers        []Workerr
	mapAllocLen    int
	reduceAllocLen int
	mapLogLen      int
	reduceLoglen   int
	lock           sync.Mutex
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Please specify input files!\n")
		os.Exit(1)
	}

	m := CreateController(os.Args[1:], 10)
	for m.Done() == false {
		time.Sleep(time.Second)
	}

	fmt.Println(m)
	time.Sleep(time.Second)
}

func (c *Controller) Done() bool {
	return c.reduceAllocLen == c.reduceLoglen
}

func CreateController(files []string, reduceNum int) *Controller {
	c := Controller{}
	c.mapAllocLen = len(files)
	c.reduceAllocLen = reduceNum
	c.mapLogLen = 0
	c.reduceLoglen = 0
	c.lock = sync.Mutex{}

	for _, v := range files {
		c.tasks = append(c.tasks, Task{-1, IDLE, MAP, v})
	}

	c.server()
	return &c
}

func (c *Controller) RegisterWorker(args *PlainArgs, reply *WorkerInitReply) error {
	c.lock.Lock()
	reply.ID = len(c.workers)
	reply.ReduceAllocLen = c.reduceAllocLen
	c.workers = append(c.workers, Workerr{reply.ID})
	c.lock.Unlock()
	fmt.Println(c)
	return nil
}

func (c *Controller) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
