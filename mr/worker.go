package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
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

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task, err := getTask()
		if err != nil {
			time.Sleep(time.Second)
			continue
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
	input, err := os.Open(task.Input[0])
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

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("mapper: %v", err)
	}

	// open R temp reduce files
	encoders := make([]*json.Encoder, 0)
	tempfiles := make([]*os.File, len(task.Output))
	for i := range task.Output {
		tempfiles[i], err = os.CreateTemp(wd, "temp-*")
		if err != nil {
			log.Fatalf("mapper %v: %v", task, err)
		}

		encoders = append(encoders, json.NewEncoder(tempfiles[i]))
	}

	// partition the keys into R files
	for _, p := range kva {
		reduceno := ihash(p.Key) % task.R
		err := encoders[reduceno].Encode(p)
		if err != nil {
			log.Fatalf("mapper: %v", err)
		}
	}

	// close the temp files and rename them
	for i, file := range tempfiles {
		err = os.Rename(file.Name(), filepath.Join(wd, task.Output[i]))
		if err != nil {
			log.Fatalf("mapper: %v", err)
		}

		file.Close()
	}

	if err := finish(task); err != nil {
		log.Fatalf("mapper: %v", err)
	}
}

func reducer(reducef func(string, []string) string, task *Task) {
	intermediate := make([]KeyValue, 0)
	for _, filename := range task.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("reducer: %v\n", err)
		}

		doc := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := doc.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

		file.Close()
	}

	sort.Sort(ByKey(intermediate))

	wd, err := os.Getwd()
	if err != nil {
		log.Fatalf("mapper: %v", err)
	}

	ofile, err := os.CreateTemp(wd, "temp-*")
	if err != nil {
		log.Fatalf("reducer: %v\n", err)
	}

	l := 0
	for l < len(intermediate) {
		r := l + 1
		for r < len(intermediate) && intermediate[r].Key == intermediate[l].Key {
			r++
		}

		values := make([]string, 0)
		for k := l; k < r; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[l].Key, values)

		// this is the correct format for each line of Reduce output.
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[l].Key, output)
		if err != nil {
			log.Fatalf("reducer: %v\n", err)
		}

		l = r
	}

	ofile.Close()

	err = os.Rename(ofile.Name(), filepath.Join(wd, task.Output[0]))
	if err != nil {
		log.Fatalf("reducer: %v", err)
	}

	if err = finish(task); err != nil {
		log.Fatalf("reducer: %v", err)
	}
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
