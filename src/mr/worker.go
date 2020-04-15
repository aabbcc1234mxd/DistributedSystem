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
	"strconv"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
//
type KeyValues []KeyValue
// for sorting by key.
func (a KeyValues) Len() int           { return len(a) }
func (a KeyValues) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a KeyValues) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// uncomment to send the Example RPC to the master.
	// CallExample()

	// Your worker implementation here.
	for {
		res := TaskRequest()
		switch res.TaskType {
		case "map":
			taskNo := res.TaskNo
			filename := res.Filename
			file, err := os.Open(filename)
			defer file.Close()
			if err != nil {
				log.Fatalf("cannot open %v", filename)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			words := mapf(filename, string(content))
			
			reducePacthes := [10]KeyValues{}
			for _, kv := range words {
				taskNum := ihash(kv.Key) % 10
				reducePacthes[taskNum] = append(reducePacthes[taskNum], kv)
			}
			for i := 0; i < 10; i++ {
				// A reasonable naming convention for intermediate files is mr-X-Y, 
				// where X is the Map task number, and Y is the reduce task number.
				oname := "mr-" + strconv.Itoa(taskNo) + "-" + strconv.Itoa(i)
				tempfile, err := ioutil.TempFile(".", "*")
				if err != nil {
					log.Fatalf("Create file failed%v", err.Error())
				}
				enc := json.NewEncoder(tempfile)
				err = enc.Encode(&reducePacthes[i])
				if err != nil {
					log.Fatalf("Encoder failed %v", err.Error())
				}
				tempfile.Close()
				os.Rename(tempfile.Name(), oname)
			}
			MapFinished(filename, res.RPCID)
			
		case "reduce":
			taskNo := res.TaskNo
			wordmaps := KeyValues{}
			for i := 0; i < 10; i++ {
				jsonname := "mr-" + strconv.Itoa(taskNo) + "-" + strconv.Itoa(i)
				jsonfile, err := os.Open(jsonname)
				if err != nil {
					log.Fatalf("Open file failed %v", err.Error())
				}
				decoder := json.NewDecoder(jsonfile)
				words := KeyValues{}
				err = decoder.Decode(&words)
				wordmaps = append(wordmaps, words...)
				if err != nil {
					log.Fatalf("Decoder failed %v", err.Error())
				}
				jsonfile.Close()
			}
			sort.Sort(KeyValues(wordmaps))
			outname := "mr-out-" + strconv.Itoa(taskNo)
			outputfile, _ := os.Create(outname)
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			for i := 0; i < len(wordmaps); {
				j := i + 1
				for j < len(wordmaps) && wordmaps[j].Key == wordmaps[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, wordmaps[k].Value)
				}
				output := reducef(wordmaps[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(outputfile, "%v %v\n", wordmaps[i].Key, output)
				i = j
			}
			outputfile.Close()
			ReduceFinished(taskNo, res.RPCID)
		default:
			os.Exit(0)
		}
	}
}
//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
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
//
func TaskRequest() *TaskAssignReply {
	// declare an argument structure,
	// fill in the arguments(but no in request here).
	args := TaskAssignArgs{}
	// declare a reply structure.
	reply := TaskAssignReply{}

	// send the RPC request, wait for the reply.
	call("Master.TaskAssign", &args, &reply)
	return &reply
}
//
func MapFinished(filename string, rpcID int) {
	args := MapFinishedArgs{}
	args.MapFilename = filename
	args.RPCID = rpcID
	reply := MapFinishedReply{}
	call("Master.MapFinish", &args, &reply)
}
//
func ReduceFinished(taskNo int, rpcID int) {
	args := ReduceFinishedArgs{}
	args.ReduceTaskNo = taskNo
	args.RPCID = rpcID
	reply := ReduceFinishedReply{}
	call("Master.MapFinish", &args, &reply)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":21234")
	//sockname := masterSock()
	//c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		// if master exits, then directly exit is ok
		os.Exit(1)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
