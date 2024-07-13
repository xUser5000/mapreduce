package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task, err := getTask()
		if err != nil {
			fmt.Printf("Worker %v: exit due to unreachable master\n", os.Getpid())
			os.Exit(0)
		}

		fmt.Printf("Worker %v: executing %v\n", os.Getpid(), task)

		if task.Type == TaskTypeMap {
			mapper(mapf, task)
		} else {
			reducer(reducef, task)
		}

		fmt.Printf("Worker %v: finished %v\n", os.Getpid(), task)
	}
}

func mapper(mapf func(string, string) []KeyValue, task *Task) {
	input, err := os.Open(task.Input)
	if err != nil {
		log.Fatalf("Worker: %v", err)
	}
	defer input.Close()

	content, err := io.ReadAll(input)
	if err != nil {
		log.Fatalf("Worker: %v", err)
	}

	// run the user-defined map function
	kva := mapf(input.Name(), string(content))

	mapno := task.Handle

	// open R reduce files
	encoders := make([]*json.Encoder, 0)
	for reduceno := range task.R {
		name := fmt.Sprintf("mr-%v-%v", mapno, reduceno)
		file, err := os.Create(name)
		if err != nil {
			log.Fatalf("mapper: %v", err)
		}
		defer file.Close()

		encoders = append(encoders, json.NewEncoder(file))
	}

	// partition the keys into R files
	for _, p := range kva {
		reduceno := ihash(p.Key) % task.R
		err := encoders[reduceno].Encode(p)
		if err != nil {
			log.Fatalf("mapper: %v", err)
		}
	}

	if err := finish(task); err != nil {
		log.Fatalf("mapper: %v", err)
	}
}

func reducer(reducef func(string, []string) string, task *Task) {
	time.Sleep(time.Millisecond * 200)
	finish(task)
}

func getTask() (*Task, error) {
	args := GetTaskArgs{Worker: strconv.Itoa(os.Getpid())}
	reply := Task{}
	if !call("Master.GetTask", &args, &reply) {
		return nil, errors.New("getTask(): something went wrong\n")
	}
	return &reply, nil
}

func finish(task *Task) error {
	args := FinishArgs{Handle: task.Handle, Type: task.Type}
	fmt.Println(args)
	reply := FinishReply{}
	if !call("Master.Finish", &args, &reply) {
		return errors.New("finish(): something went wrong\n")
	}
	return nil
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
