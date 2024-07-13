package mr

type Task struct {
	Handle int
	R      int
	Type   TaskType
	Input  string
	Output string
	Status TaskStatus
	Worker string
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
