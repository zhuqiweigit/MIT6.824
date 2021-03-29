package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const MaxIdleSleepSeconds int = 1

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type KeyValueArray []KeyValue

func (kva KeyValueArray) Len() int {
	return len(kva)
}

func (kva KeyValueArray) Swap(i, j int) {
	kva[i], kva[j] = kva[j], kva[i]
}

func (kva KeyValueArray) Less(i, j int) bool {
	return kva[i].Key < kva[j].Key
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		request := TaskRequest{}
		response := TaskResponse{}
		callResult := call("Master.AskForTask", &request, &response)
		if callResult == false {
			fmt.Printf("failed to request task \n")
			os.Exit(-1)
		}
		if response.TaskType == IDLE_TASK {

			//fmt.Printf("idle Task\n")

			time.Sleep(time.Second * MaxTaskExecuteSeconds)
			continue
		} else if response.TaskType == MAP_TASK {

			//fmt.Printf("map Task %v \n", response)

			fileName := response.FileName
			mapTaskId := response.TaskId
			NReduce := response.NReduce

			doMapJob(fileName, mapTaskId, NReduce, mapf)

			request := CommitRequest{response.TaskType, response.TaskId}
			response := CommitResponse{}
			call("Master.CommitTask", &request, &response)

		} else if response.TaskType == REDUCE_TASK {

			//fmt.Printf("reduce Task %v\n", response)

			reduceTaskId := response.TaskId

			doReduceJob(reduceTaskId, reducef)

			request := CommitRequest{response.TaskType, response.TaskId}
			response := CommitResponse{}
			call("Master.CommitTask", &request, &response)

		}
	}
}

func readFileWithBytes(fileName string) []byte {
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Printf("open file [%v] failed  %v", fileName, err)
		os.Exit(-1)
	}

	defer file.Close()

	buf, err := ioutil.ReadAll(file)
	if err != nil {
		fmt.Printf("read file [%v] failed  %v", fileName, err)
		os.Exit(-1)
	}
	return buf
}

func saveMapIntermediate(mapTaskId int, NReduce int, kva []KeyValue) {
	tempMidFiles := make([]string, NReduce)
	for i := 0; i < NReduce; i++ {
		tempMidFiles[i] = fmt.Sprintf("tmp-mr-%v-%v", mapTaskId, i)
		tempf, err := os.Create("./" + tempMidFiles[i])
		if err != nil {
			log.Fatalf("create tmp map file [%v] err", tempMidFiles[i], err)
		}
		tempf.Close()
	}

	for ll := 0; ll < len(kva); {
		rr := ll + 1
		for ; rr < len(kva) && kva[ll].Key == kva[rr].Key; rr++ {
		}
		hashVal := ihash(kva[ll].Key)
		reduceId := hashVal % NReduce
		writef, err := os.OpenFile(tempMidFiles[reduceId], os.O_APPEND|os.O_RDWR, os.ModePerm)
		if err != nil {
			log.Fatalf("Open temp file [%v] err,  %v", tempMidFiles[reduceId], err)
		}
		encoder := json.NewEncoder(writef)
		for idx := ll; idx < rr; idx++ {
			err := encoder.Encode(kva[idx])
			if err != nil {
				fmt.Printf("encode kv fail %v\n", err)
				os.Exit(-1)
			}
		}
		writef.Close()
		ll = rr
	}

	for reduceIdx, tempName := range tempMidFiles {
		newName := fmt.Sprintf("mr-%v-%v", mapTaskId, reduceIdx)
		os.Rename("./"+tempName, "./"+newName)
	}
}

func doMapJob(fileName string, taskId int, NReduce int,
	mapf func(string, string) []KeyValue) {

	buf := readFileWithBytes(fileName)
	contents := string(buf)

	kva := mapf(fileName, contents)
	sort.Sort(KeyValueArray(kva))

	saveMapIntermediate(taskId, NReduce, kva)
}

func readMapIntermediate(reduceTaskId int) []KeyValue {

	allFiles, err := ioutil.ReadDir("./")
	if err != nil {
		fmt.Printf("open dir fail [%v]\n", err)
		os.Exit(-1)
	}

	kva := make([]KeyValue, 0)

	for _, file := range allFiles {
		map_id := -1
		reduce_id := -1
		fileName := file.Name()
		n, _ := fmt.Sscanf(fileName, "mr-%v-%v", &map_id, &reduce_id)
		if n < 2 || reduce_id != reduceTaskId {
			continue
		}

		f, err := os.Open("./" + fileName)
		if err != nil {
			fmt.Printf("open file fail [%v]\n", err)
			os.Exit(-1)
		}

		decoder := json.NewDecoder(f)
		for {
			kv := KeyValue{}
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	return kva
}

func doReduceJob(taskId int, reducef func(string, []string) string) {

	reduceFileNameTmp := fmt.Sprintf("tmp-mr-out-%v", taskId)
	reduceFileName := fmt.Sprintf("mr-out-%v", taskId)

	reduceFile, err := os.Create("./" + reduceFileNameTmp)
	if err != nil {
		fmt.Printf("create reduce file error %v", err)
		os.Exit(-1)
	}

	defer reduceFile.Close()

	kva := readMapIntermediate(taskId)

	sort.Sort(KeyValueArray(kva))

	for ll := 0; ll < len(kva); {
		rr := ll
		vals := make([]string, 0)
		for ; rr < len(kva) && kva[ll].Key == kva[rr].Key; rr++ {
			vals = append(vals, kva[rr].Value)
		}

		reduceResult := reducef(kva[ll].Key, vals)

		fmt.Fprintf(reduceFile, "%v %v\n", kva[ll].Key, reduceResult)

		ll = rr
	}

	os.Rename("./"+reduceFileNameTmp, "./"+reduceFileName)
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		os.Exit(-1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
