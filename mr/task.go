package mr

import "fmt"

type Task struct {
	Handle int
	R      int
	M      int
	Type   TaskType
	Input  []string
	Output []string
	Status TaskStatus
	Worker string
}

func (task *Task) String() string {
	return fmt.Sprintf("Task{ Type: %s, Handle: %v }", task.Type, task.Handle)
}

type TaskType int

const (
	TaskTypeMap TaskType = iota
	TaskTypeReduce
)

var taskTypeName = map[TaskType]string{
	TaskTypeMap:    "map",
	TaskTypeReduce: "reduce",
}

func (taskType TaskType) String() string {
	return taskTypeName[taskType]
}

type TaskStatus int

const (
	TaskStatusReady TaskStatus = iota
	TaskStatusInProgress
	TaskStatusFinished
)

var taskStatusName = map[TaskStatus]string{
	TaskStatusReady:      "ready",
	TaskStatusInProgress: "in_progress",
	TaskStatusFinished:   "finished",
}

func (taskStatus TaskStatus) String() string {
	return taskStatusName[taskStatus]
}

type Phase int

const (
	PhaseMap Phase = iota
	PhaseReduce
	PhaseFinished
)

var phaseName = map[Phase]string{
	PhaseMap:      "map",
	PhaseReduce:   "reduce",
	PhaseFinished: "finished",
}

func (phase Phase) String() string {
	return phaseName[phase]
}
