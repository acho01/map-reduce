package main

import (
	"os"
	"strconv"
)

type PlainArgs struct{}
type PlainReply struct{}

type WorkerInitReply struct {
	ID         int
	ReduceAllocLen int
	OutFile    string
}

type GetTaskArgs struct {
	ID int
}

type GetTaskReply struct {
	File  string
	Ttype int
}

type TaskCompleteArgs struct {
	Id    int
	Ttype int
}

func coordinatorSock() string {
	s := "/var/tmp/mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
