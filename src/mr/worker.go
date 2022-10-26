package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strings"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	id      int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.register()
	w.run()
	// uncomment to send the Example RPC to the master.
	// CallExample()
}

// Run task got from maters
func (w *worker) run() {
	for {
		task := w.reqTask()
		if !task.Alive {
			return
		}
		w.doTask(task)
	}
}

// Requst task from maters
func (w *worker) reqTask() Task {
	args := TaskArgs{}
	args.WorkerId = w.id
	reply := TaskReply{}

	if ok := call("Master.ArrangeOneTask", &args, &reply); !ok {
		DPrintf("worker get task fail,exit")
		os.Exit(1)
	}
	DPrintf("worker get task:%+v", reply.Task)
	return *reply.Task
}

func (w *worker) doTask(t Task) {
	DPrintf("in do Task")
	switch t.Type {
	case MapType:
		w.doMapTask(t)
	case ReduceType:
		w.doReduceTask(t)
	default:
		panic(fmt.Sprintf("task phase err: %v", t.Type))
	}
}

// Map core function
func (w *worker) doMapTask(t Task) {
	contents, err := ioutil.ReadFile(t.FileName)
	if err != nil {
		w.reportTask(t, false, err)
		return
	}
	// Use mapfunction to compute kvs, k is key(word), v is value('1')
	// map核心函数调用
	kvs := w.mapf(t.FileName, string(contents))
	reduces := make([][]KeyValue, t.NReduce)
	// 一个mapTask要把结果存成nReduce份, 根据ihash划分
	for _, kv := range kvs {
		idx := ihash(kv.Key) % t.NReduce
		//包含对应的reducerID以让之后的reduce任务知道自己要读取哪些中间结果
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, l := range reduces {
		// 生成中间文件名：mr-mapTaskID-reduceID
		fileName := reduceName(t.Seq, idx)
		f, err := os.Create(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(t, false, err)
			}

		}
		if err := f.Close(); err != nil {
			w.reportTask(t, false, err)
		}
	}
	w.reportTask(t, true, nil)

}

// Reduce core function
func (w *worker) doReduceTask(t Task) {
	maps := make(map[string][]string)
	for idx := 0; idx < t.NMaps; idx++ {
		// 根据文件名打开需要reduce的中间文件
		fileName := reduceName(idx, t.Seq) //对应maptask中的t.seq和idx
		file, err := os.Open(fileName)
		if err != nil {
			w.reportTask(t, false, err)
			return
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	res := make([]string, 0, 100)
	// reduce 核心函数调用
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}

	if err := ioutil.WriteFile(mergeName(t.Seq), []byte(strings.Join(res, "")), 0600); err != nil {
		w.reportTask(t, false, err)
	}

	w.reportTask(t, true, nil)
}

// Report the completion of the task to the master
// If something wrong happened during the process, report to master
func (w *worker) reportTask(t Task, done bool, err error) {
	if err != nil {
		log.Printf("%v", err)
	}
	args := ReportTaskArgs{}
	args.Done = done
	args.Seq = t.Seq
	args.Type = t.Type
	args.WorkerId = w.id
	reply := ReportTaskReply{}
	if ok := call("Master.ReceiveTask", &args, &reply); !ok {
		DPrintf("report task fail:%+v", args)
	}
}

// Registe worker to master to require task
func (w *worker) register() {
	args := &RegisterArgs{}
	reply := &RegisterReply{}
	if ok := call("Master.RegWorker", args, reply); !ok {
		log.Fatal("reg fail")
	}
	w.id = reply.WorkerId
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
