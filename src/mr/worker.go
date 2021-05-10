package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func FNameInter(ID string, reduceID int) string {
	return fmt.Sprintf("%s.%d.inter", ID, reduceID)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		succ := call("Coordinator.GetTask", &args, &reply)
		if !succ || reply.Finished {
			log.Println("No more jobs, exiting graciously!")
			os.Exit(0)
		}

		task := reply.TaskInstance
		if (task == Task{}) {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if task.Type == MapTaskT {
			HandleMapTask(mapf, &task)
		} else if task.Type == ReduceTaskT {
			HandleReduceTask(reducef, &task)
		}
		SendSubmitTask(&task)
	}
}

func SendSubmitTask(task *Task) {
	args := SubmitTaskArgs{ID: task.ID, Type: task.Type}
	reply := SubmitTaskReply{}
	succ := call("Coordinator.SubmitTask", &args, &reply)

	if !succ || reply.Finished {
		log.Println("No more jobs, exiting graciously!")
		os.Exit(0)
	}
}

func HandleMapTask(mapf func(string, string) []KeyValue, task *Task) {
	kvmap := map[string][]KeyValue{}
	contents, _ := ioutil.ReadFile(task.FName)
	kvs := mapf(task.FName, string(contents))
	// Write K V to file
	for _, kv := range kvs {
		fnameInter := FNameInter(fmt.Sprint(task.ID), ihash(kv.Key)%task.NReduce)
		kvmap[fnameInter] = append(kvmap[fnameInter], kv)
	}
	for fname, kvs := range kvmap {
		file, _ := os.Create(fname)
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range kvs {
			enc.Encode(&kv)
		}
	}
}

func HandleReduceTask(reducef func(string, []string) string, task *Task) {
	intermediate := []KeyValue{}
	fnames, _ := filepath.Glob(FNameInter("*", task.ID))

	for _, fname := range fnames {
		file, _ := os.Open(fname)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}

	}
	sort.Sort(ByKey(intermediate))
	results := make(map[string]string)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		results[intermediate[i].Key] = output
		// this is the correct format for each line of Reduce output.
		i = j
	}
	ofile, _ := os.Create(fmt.Sprintf("mr-out-%d", task.ID))
	defer ofile.Close()
	for k, v := range results {
		fmt.Fprintf(ofile, "%v %v\n", k, v)
	}

}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
