# Map-Reduce

Current distributed map-reduce implementation is based on google's paper: https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf
No fault tolerance, or real-life issues is supported in this implementation.
Framework is fully implemented in Golang. 

## How to run

To start controller server with test input files you can run this commmand:
go run commons.go rpc.go controller.go ./input_files/pg-*.txt

You can simulation distrubuted behaviour by creating several worker processes.
You can run this command to create single worker:
go run commons.go rpc.go worker.go
