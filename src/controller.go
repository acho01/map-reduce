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

func (c *Controller) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.lock.Lock()
	defer c.lock.Unlock()
	for i := range c.tasks {
		t := &c.tasks[i]
		if t.status == IDLE {
			t.status = IN_PROGRESS
			t.workerId = args.ID
			reply.Ttype = t.ttype
			reply.File = t.file
			break
		}
	}

	fmt.Println(c)
	return nil
}

func (c *Controller) TaskComplete(args *TaskCompleteArgs, reply *PlainReply) error {
	c.lock.Lock()
	if args.Ttype == MAP {
		c.mapLogLen++

		// If all map tasks are done, procceed with reduce taks
		if c.mapAllocLen == c.mapLogLen {
			for i := 0; i < c.reduceAllocLen; i++ {
				c.tasks = append(c.tasks, Task{-1, IDLE, REDUCE, fmt.Sprintf("%v", i)})
			}
		}
	} else {
		c.reduceLoglen++
	}
	c.lock.Unlock()

	for i, _ := range c.tasks {
		c.lock.Lock()
		t := &c.tasks[i]
		if t.status == IDLE && t.workerId == args.Id {
			t.status = COMPLETED
			c.lock.Unlock()
			break
		}
		c.lock.Unlock()
	}
	return nil
}

func (c *Controller) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}
