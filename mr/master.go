package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"slices"
	"sync"
	"time"
)

type Master struct {
	Phase       Phase
	R           int
	M           int
	MapTasks    []Task
	ReduceTasks []Task
	Queue       chan *Task
	Mutex       sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) GetTask(args *GetTaskArgs, reply *Task) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	if m.doneWithoutLocks() || len(m.Queue) == 0 {
		return errors.New("master: there are no available tasks")
	}

	selectedTask := <-m.Queue
	selectedTask.Worker = args.Worker
	selectedTask.Status = TaskStatusInProgress

	// wait ten seconds for the worker to complete its task
	// if it didn't, simply make the task available again
	// so that it can be picked up by other workers
	selectedTask.timer = time.NewTimer(10 * time.Second)
	go func() {
		<-selectedTask.timer.C
		m.Mutex.Lock()
		defer m.Mutex.Unlock()
		selectedTask.Worker = ""
		selectedTask.Status = TaskStatusReady
		selectedTask.timer = nil
		m.Queue <- selectedTask
	}()

	*reply = *selectedTask
	return nil
}

func (m *Master) Finish(args FinishArgs, reply *FinishReply) error {
	m.Mutex.Lock()
	defer m.Mutex.Unlock()

	var task *Task
	if args.Type == TaskTypeMap {
		task = &m.MapTasks[args.Handle]
	} else {
		task = &m.ReduceTasks[args.Handle]
	}
	task.Status = TaskStatusFinished
	task.Worker = ""
	if task.timer != nil {
		task.timer.Stop()
	}

	finishedMap := !slices.ContainsFunc(m.MapTasks, func(task Task) bool {
		return task.Status != TaskStatusFinished
	})
	if m.Phase == PhaseMap && finishedMap {
		for i := range m.R {
			input := make([]string, 0)
			for j := range m.M {
				input = append(input, fmt.Sprintf("mr-%v-%v", j, i))
			}

			output := []string{fmt.Sprintf("mr-out-%v", i)}

			reduceTask := Task{
				Handle: i,
				M:      m.M,
				R:      m.R,
				Type:   TaskTypeReduce,
				Input:  input,
				Output: output,
				Status: TaskStatusReady,
				Worker: "",
			}
			m.ReduceTasks = append(m.ReduceTasks, reduceTask)
			m.Queue <- &reduceTask
		}
		m.Phase = PhaseReduce
	}

	finishedReduce := !slices.ContainsFunc(m.ReduceTasks, func(task Task) bool {
		return task.Status != TaskStatusFinished
	})
	if m.Phase == PhaseReduce && finishedReduce {
		m.Phase = PhaseFinished
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
	m.Mutex.Lock()
	defer m.Mutex.Unlock()
	return m.doneWithoutLocks()
}

func (m *Master) doneWithoutLocks() bool {
	return m.Phase == PhaseFinished
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		Phase:       PhaseMap,
		MapTasks:    make([]Task, 0),
		ReduceTasks: make([]Task, 0),
		R:           nReduce,
		M:           len(files),
		Queue:       make(chan *Task, nReduce+len(files)),
	}

	for i, file := range files {
		output := make([]string, 0)
		for j := range m.R {
			output = append(output, fmt.Sprintf("mr-%v-%v", i, j))
		}

		task := Task{
			Handle: i,
			M:      m.M,
			R:      m.R,
			Type:   TaskTypeMap,
			Input:  []string{file},
			Output: output,
			Status: TaskStatusReady,
			Worker: "",
		}
		m.MapTasks = append(m.MapTasks, task)
		m.Queue <- &task
	}

	m.server()
	return &m
}
