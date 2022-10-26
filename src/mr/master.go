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
	TaskStatusReady   = 0
	TaskStatusQueue   = 1
	TaskStatusRunning = 2
	TaskStatusFinish  = 3
	TaskStatusErr     = 99
)

const (
	MaxTaskRunTime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskStat struct {
	Status    int
	WorkerId  int
	StartTime time.Time
}

type Master struct {
	// Your definitions here.
	Files     []string
	nReduce   int
	taskType  TaskType
	taskStats []TaskStat
	mu        sync.Mutex
	done      bool
	workerSeq int
	taskCh    chan Task // Task queue
}

// Get the seq task from master
func (m *Master) getTask(taskSeq int) Task {
	// Create an empty task
	task := Task{
		FileName: "",
		NReduce:  m.nReduce,
		NMaps:    len(m.Files),
		Seq:      taskSeq,
		Type:     m.taskType,
		Alive:    true,
	}
	DPrintf("m:%+v, taskseq:%d, lenfiles:%d, lents:%d", m, taskSeq, len(m.Files), len(m.taskStats))
	if task.Type == MapType {
		task.FileName = m.Files[taskSeq]
	}
	return task
}

// 处理tasks
func (m *Master) schedule() {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.done {
		return
	}
	allFinish := true
	for index, t := range m.taskStats {
		switch t.Status {
		// Task 放入等待对列
		case TaskStatusReady:
			allFinish = false
			m.taskCh <- m.getTask(index)
			m.taskStats[index].Status = TaskStatusQueue
		case TaskStatusQueue:
			allFinish = false
		case TaskStatusRunning:
			allFinish = false
			// 超时处理
			if time.Now().Sub(t.StartTime) > MaxTaskRunTime {
				m.taskStats[index].Status = TaskStatusQueue
				m.taskCh <- m.getTask(index)
			}
		case TaskStatusFinish:
		case TaskStatusErr:
			allFinish = false
			m.taskStats[index].Status = TaskStatusQueue
			m.taskCh <- m.getTask(index)
		default:
			panic("t.status err")
		}
	}
	if allFinish {
		if m.taskType == MapType {
			m.initReduceTask()
		} else {
			m.done = true
		}
	}
}

func (m *Master) initMapTask() {
	m.taskType = MapType
	m.taskStats = make([]TaskStat, len(m.Files))
}

func (m *Master) initReduceTask() {
	DPrintf("init ReduceTask")
	m.taskType = ReduceType
	m.taskStats = make([]TaskStat, m.nReduce)
}

// Register one task
func (m *Master) regTask(args *TaskArgs, task *Task) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if task.Type != m.taskType {
		panic("Aragne Task Type not Equal")
	}
	//给分配好worker的task初始化状态
	m.taskStats[task.Seq].Status = TaskStatusRunning
	m.taskStats[task.Seq].WorkerId = args.WorkerId
	m.taskStats[task.Seq].StartTime = time.Now()
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// Arrange a task to worker
func (m *Master) ArrangeOneTask(args *TaskArgs, reply *TaskReply) error {
	//从task等待对列中取task分配给worker
	task := <-m.taskCh
	reply.Task = &task

	if task.Alive {
		m.regTask(args, &task)
	}
	DPrintf("in get one Task, args:%+v, reply:%+v", args, reply)
	return nil
}

// Receive completed/error task from worker
func (m *Master) ReceiveTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	DPrintf("get report task: %+v, taskPhase: %+v", args, m.taskType)

	if m.taskType != args.Type || args.WorkerId != m.taskStats[args.Seq].WorkerId {
		return nil
	}

	if args.Done {
		m.taskStats[args.Seq].Status = TaskStatusFinish
	} else {
		m.taskStats[args.Seq].Status = TaskStatusErr
	}

	go m.schedule()
	return nil
}

// Register worker
func (m *Master) RegWorker(args *RegisterArgs, reply *RegisterReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.workerSeq += 1
	reply.WorkerId = m.workerSeq
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.done
}

func (m *Master) tickSchedule() {
	// 每个 task 一个 timer
	for !m.Done() {
		go m.schedule()
		time.Sleep(ScheduleInterval)
	}
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu = sync.Mutex{}
	m.nReduce = nReduce
	m.Files = files
	// 创建任务管道
	if nReduce > len(files) {
		m.taskCh = make(chan Task, nReduce)
	} else {
		m.taskCh = make(chan Task, len(files))
	}
	m.initMapTask()
	m.tickSchedule()

	m.server()
	return &m
}
