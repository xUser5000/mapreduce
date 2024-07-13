package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"time"
)
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	MapTasks    []Task
	ReduceTasks []Task
	Phase       TaskType
	R           int
	M           int
	TaskCounter int
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *Task) error {
	var selectedTask *Task

	if m.Phase == TaskTypeMap {
		for i := 0; i < len(m.MapTasks); i++ {
			if m.MapTasks[i].Status == TaskStatusReady {
				selectedTask = &m.MapTasks[i]
				break
			}
		}

		if selectedTask == nil {
			for m.Phase == TaskTypeMap {
				time.Sleep(time.Second)
			}
		}
	}

	if selectedTask == nil {
		for i := 0; i < len(m.ReduceTasks); i++ {
			if m.ReduceTasks[i].Status == TaskStatusReady {
				selectedTask = &m.ReduceTasks[i]
				break
			}
		}
	}

	if selectedTask == nil {
		return errors.New("master: there are no available tasks\n")
	}

	selectedTask.Worker = args.Worker
	selectedTask.Status = TaskStatusInProgress

	*reply = *selectedTask
	return nil
}

func (m *Master) Finish(args FinishArgs, reply *FinishReply) error {
	var task *Task
	if args.Type == TaskTypeMap {
		task = &m.MapTasks[args.Handle]
	} else {
		task = &m.ReduceTasks[args.Handle]
	}
	task.Status = TaskStatusFinished
	task.Worker = ""

	finishedMaps := 0
	for _, task := range m.MapTasks {
		if task.Status == TaskStatusFinished {
			finishedMaps++
		}
	}

	if m.Phase == TaskTypeMap && finishedMaps == len(m.MapTasks) {
		for i := range m.R {
			input := make([]string, 0)
			for j := range m.M {
				input = append(input, fmt.Sprintf("mr-%v-%v", j, i))
			}

			output := []string{fmt.Sprintf("mr-out-%v", i)}

			reduceTask := Task{
				Handle: i,
				R:      m.R,
				Type:   TaskTypeReduce,
				Input:  input,
				Output: output,
				Status: TaskStatusReady,
				Worker: "",
			}
			m.ReduceTasks = append(m.ReduceTasks, reduceTask)
		}
		m.Phase = TaskTypeReduce
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	if m.Phase == TaskTypeReduce {
		for _, task := range m.ReduceTasks {
			if task.Status != TaskStatusFinished {
				return false
			}
		}
		return true
	}
	return false
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Phase:       TaskTypeMap,
		MapTasks:    make([]Task, 0),
		ReduceTasks: make([]Task, 0),
		R:           nReduce,
		M:           len(files),
		TaskCounter: 0,
	}

	for i, file := range files {
		output := make([]string, 0)
		for j := range m.R {
			output = append(output, fmt.Sprintf("mr-%v-%v", i, j))
		}

		task := Task{
			Handle: i,
			R:      nReduce,
			Type:   TaskTypeMap,
			Input:  []string{file},
			Output: output,
			Status: TaskStatusReady,
			Worker: "",
		}
		m.MapTasks = append(m.MapTasks, task)
	}

	m.server()
	return &m
}
