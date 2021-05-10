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

type Coordinator struct {
	// Your definitions here.
	phase Phase
	mutex sync.Mutex

	mapTasks map[int]*Task

	reduceTasks map[int]*Task

	files   []string
	nReduce int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) SubmitTask(args *SubmitTaskArgs, reply *SubmitTaskReply) error {
	ID := args.ID
	Type := args.Type
	log.Printf("Phase %d: Marking task %v as Done\n", c.phase, ID)
	c.mutex.Lock()
	var task *Task
	if Type == MapTaskT {
		task = c.mapTasks[ID]
	} else if Type == ReduceTaskT {
		task = c.reduceTasks[ID]
	}
	// if task.Status != InProgress {
	// 	log.Printf("Phase %d: Rejected attempt to set 'Done' on Task that is not 'InProgress'...\n", c.phase)
	// 	return nil
	// }
	task.Status = Done
	c.mutex.Unlock()
	if c.phase == ReducePhase && c.CheckAllDone(c.reduceTasks) {
		c.InitDone()
		reply.Finished = true
	}
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	var tasks map[int]*Task
	if c.phase == MapPhase {
		tasks = c.mapTasks
	} else if c.phase == ReducePhase {
		tasks = c.reduceTasks
	} else if c.phase == DonePhase {
		reply.Finished = true
		return nil
	}

	for _, task := range tasks {
		if task.Status == Pending {
			// log.Printf("Phase %d: Sending task %v to worker\n", c.phase, ID)
			c.mutex.Lock()
			task.Status = InProgress
			task.Started = time.Now()
			c.mutex.Unlock()
			reply.TaskInstance = *task
			return nil
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) CheckAllDone(tasks map[int]*Task) bool {
	todo := len(tasks)
	for _, task := range tasks {
		if task.Status == Done {
			todo -= 1
		}
	}
	return todo == 0
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (c *Coordinator) InitReduce() error {
	c.mutex.Lock()
	c.phase = ReducePhase
	for ID := 0; ID < c.nReduce; ID++ {
		task := &Task{
			ID:      ID,
			Type:    ReduceTaskT,
			Status:  Pending,
			Started: time.Now(),
			NReduce: c.nReduce,
		}
		c.reduceTasks[ID] = task
	}
	c.mutex.Unlock()
	return nil
}

func (c *Coordinator) InitDone() error {
	c.mutex.Lock()
	c.phase = DonePhase
	c.mutex.Unlock()
	return nil
}
func (c *Coordinator) TimeOutTasks(tasks map[int]*Task) error {
	for _, task := range tasks {
		if task.Status == InProgress && time.Since(task.Started).Seconds() > 10 {
			c.mutex.Lock()
			task.Status = Pending
			task.Started = time.Time{}
			c.mutex.Unlock()
		}
	}
	return nil
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// mark timed out tasks as pending

	if c.phase == MapPhase {
		c.TimeOutTasks(c.mapTasks)
		if c.CheckAllDone(c.mapTasks) {
			c.InitReduce()
		}
	}
	if c.phase == ReducePhase {
		c.TimeOutTasks(c.reduceTasks)
		if c.CheckAllDone(c.reduceTasks) {
			c.InitDone()
		}
	}
	return c.phase == DonePhase
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{mapTasks: map[int]*Task{}, reduceTasks: map[int]*Task{}, files: files, nReduce: nReduce}

	// Your code here.
	id := 0
	for _, file := range files {
		task := &Task{ID: id, Type: MapTaskT, FName: file, Status: Pending, NReduce: nReduce}
		c.mutex.Lock()
		c.mapTasks[id] = task
		c.mutex.Unlock()
		id += 1
	}

	c.server()
	return &c
}
