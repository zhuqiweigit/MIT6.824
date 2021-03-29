package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskReady       = 0
	TaskDistributed = 1
	TaskFinished    = 2
)

const (
	MaxTaskExecuteSeconds = 10
)

type Task struct {
	taskType_ int

	taskId_ int

	taskState_ int
}

type Master struct {
	// Your definitions here.
	NReduce int

	files []string

	mapTasks   []Task
	mapTaskPtr int

	reduceTasks   []Task
	reduceTaskPtr int

	mapCompleteCnt int

	reduceCompleteCnt int

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (m *Master) AskForTask(request *TaskRequest, response *TaskResponse) error {

	//if all the reduce job finished, we will return nil to shutdown master and worker
	if m.allReduceJobFinished() {
		response.TaskType = IDLE_TASK
		return nil
	}

	//all the map job finished, we return reduce job
	if m.allMapJobFinished() {
		todoTask := m.scanAndRequireTask(REDUCE_TASK)
		response.TaskId = todoTask.taskId_
		response.TaskType = todoTask.taskType_
		if todoTask.taskType_ == IDLE_TASK {
			return nil
		}
		go m.waitForWorkerFinish(todoTask)
	} else {
		todoTask := m.scanAndRequireTask(MAP_TASK)
		response.TaskId = todoTask.taskId_
		response.TaskType = todoTask.taskType_
		response.NReduce = m.NReduce

		if todoTask.taskType_ == IDLE_TASK {
			return nil
		}
		response.FileName = m.files[todoTask.taskId_]

		go m.waitForWorkerFinish(todoTask)
	}

	return nil
}

func (m *Master) CommitTask(request *CommitRequest, response *CommitResponse) error {
	if request.TaskType == MAP_TASK {
		if m.allMapJobFinished() {
			response.Succeed = false
		} else {
			state := m.setTaskFinished(request.TaskType, request.TaskId)
			response.Succeed = state
		}
	} else if request.TaskType == REDUCE_TASK {
		if m.allReduceJobFinished() {
			response.Succeed = false
		} else {
			state := m.setTaskFinished(request.TaskType, request.TaskId)
			response.Succeed = state
		}
	} else {
		response.Succeed = false
	}

	return nil
}

func (m *Master) waitForWorkerFinish(task Task) {

	timer := time.NewTimer(time.Second * MaxTaskExecuteSeconds)
	<-timer.C

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if task.taskType_ == MAP_TASK {
		if m.mapTasks[task.taskId_].taskState_ != TaskFinished {
			m.mapTasks[task.taskId_].taskState_ = TaskReady
		}
	} else if task.taskType_ == REDUCE_TASK {
		if m.reduceTasks[task.taskId_].taskState_ != TaskFinished {
			m.reduceTasks[task.taskId_].taskState_ = TaskReady
		}
	}
}

func (m *Master) setTaskFinished(taskType int, taskId int) bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if taskType == MAP_TASK {
		if m.mapTasks[taskId].taskState_ == TaskDistributed {
			m.mapTasks[taskId].taskState_ = TaskFinished
			m.mapCompleteCnt++
			return true
		} else {
			return false
		}
	} else if taskType == REDUCE_TASK {
		if m.reduceTasks[taskId].taskState_ == TaskDistributed {
			m.reduceTasks[taskId].taskState_ = TaskFinished
			m.reduceCompleteCnt++
			return true
		} else {
			return false
		}
	}
	return false
}

func (m *Master) allMapJobFinished() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.mapCompleteCnt == len(m.mapTasks)
}

func (m *Master) allReduceJobFinished() bool {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	return m.reduceCompleteCnt == len(m.reduceTasks)
}

func (m *Master) scanAndRequireTask(TaskType int) Task {

	m.mutex.Lock()
	defer m.mutex.Unlock()

	if TaskType == MAP_TASK {
		for i := 1; i <= len(m.mapTasks); i++ {
			m.mapTaskPtr = (m.mapTaskPtr + 1) % len(m.mapTasks)
			if m.mapTasks[m.mapTaskPtr].taskState_ == TaskReady {
				m.mapTasks[m.mapTaskPtr].taskState_ = TaskDistributed
				return m.mapTasks[m.mapTaskPtr]
			}
		}
	} else if TaskType == REDUCE_TASK {
		for i := 1; i < len(m.reduceTasks); i++ {
			m.reduceTaskPtr = (m.reduceTaskPtr + 1) % len(m.reduceTasks)
			if m.reduceTasks[m.reduceTaskPtr].taskState_ == TaskReady {
				m.reduceTasks[m.reduceTaskPtr].taskState_ = TaskDistributed
				return m.reduceTasks[m.reduceTaskPtr]
			}
		}
	}

	// all job distributed doesn't means we really finish all job, because some worker would crushed and their job will fail
	// so we need let worker to be idle, until master receive all the job commit information
	return Task{IDLE_TASK, -1, -1}
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	return m.allMapJobFinished() && m.allReduceJobFinished()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {

	master := Master{}

	master.files = files
	master.mapCompleteCnt = 0
	master.reduceCompleteCnt = 0
	master.mutex = sync.Mutex{}
	master.NReduce = nReduce

	for mapId, _ := range files {
		task := Task{MAP_TASK, mapId, TaskReady}
		master.mapTasks = append(master.mapTasks, task)
	}
	master.mapTaskPtr = -1

	for i := 0; i < nReduce; i++ {
		task := Task{REDUCE_TASK, i, TaskReady}
		master.reduceTasks = append(master.reduceTasks, task)
	}
	master.reduceTaskPtr = -1

	master.server()
	return &master
}
