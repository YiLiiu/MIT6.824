package mr

import (
	"fmt"
	"log"
)

// Type of task
type TaskType int

const (
	MapType    TaskType = 0
	ReduceType TaskType = 1
)

// Debug mode
const Debug = false

func DPrintf(format string, v ...interface{}) {
	if Debug {
		log.Printf(format+"\n", v...)
	}
}

type Task struct {
	FileName string // Every file should be a task
	NReduce  int    // 一个mapTask要把结果存成nReduce份
	NMaps    int    // Number of files
	Seq      int
	Type     TaskType
	Alive    bool // worker should exit when alive is false
}

// 需要reduce的中间结果命名为：mr-mapTaskID-reduceID
func reduceName(mapIdx, reduceIdx int) string {
	return fmt.Sprintf("mr-%d-%d", mapIdx, reduceIdx)
}

// 最终结果命名：mr-out-reduceTaskID
func mergeName(reduceIdx int) string {
	return fmt.Sprintf("mr-out-%d", reduceIdx)
}
