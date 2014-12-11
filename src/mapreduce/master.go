package mapreduce
import "container/list"
import "fmt"
import "reflect"
import "strconv"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}

const NOMOREJOBS = 777
const SENDAGAIN  = 888

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) acceptWorker(comm chan string, quit chan int){
	for true{
		select{
			case v := <-mr.registerChannel:
			mr.Workers[v] = new(WorkerInfo)
	  		mr.Workers[v].address = v
	  		comm <- v
	  		case <- quit:
	  		DPrintln("quit accepetWorker")
	  		return 
		}
	}
}

func (mr *MapReduce)callWorker(targetWorker string, comm chan int){
	for true {
		jobNumber := <-comm
		djob := jobNumber
		DPrintf("recv jobnumber %v\n", djob)
		if jobNumber == NOMOREJOBS{
			DPrintf("callworker routine exit received ter job\n")
			return
		}
		if jobNumber == SENDAGAIN {
			DPrintf("callworker routine send again job\n")
			comm <- SENDAGAIN
			continue
		}
		var numOtherPhase int
		var operation JobType
		
		if jobNumber < mr.nMap {
			numOtherPhase = mr.nReduce
			operation = Map
		} else {
			jobNumber = jobNumber - mr.nMap
			numOtherPhase = mr.nMap
			operation = Reduce
		}
		
		args := new(DoJobArgs)
		args.File = mr.file
		args.JobNumber = jobNumber
		args.NumOtherPhase = numOtherPhase
		args.Operation = operation
		reply := new(DoJobReply)
		ret := call(targetWorker, "Worker.DoJob", args, reply)
		if ret == false {
			fmt.Println("in func :callworker call failed")
			comm <- -djob
		} else {
			fmt.Println("theworkerdojobsucceed " + strconv.Itoa(djob) + " done")
			comm <- djob
		}
	}	
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  DPrintf("@master@@@@@@@@@@@@@@@run master")

  accWork := make(chan string)
  quit := make(chan int)
  go mr.acceptWorker(accWork, quit)
  selCase := make([]reflect.SelectCase, 1)
  selCase[0] = reflect.SelectCase{Dir:reflect.SelectRecv, Chan: reflect.ValueOf(accWork)} 
  

  for phase := 0; phase < 2; phase++{
	  jobSet := make(map[int]bool)	
	  jobLst := list.New()
	  var jobLen int
	  if phase == 0 {
	  	jobLen = mr.nMap 
	  	for i := 0; i < jobLen; i++ {
	  		jobSet[i] = false;
	  		jobLst.PushBack(i)
	  	}
	  } else {
	  	jobLen = mr.nReduce
	  	for i := 0; i < jobLen; i++ {
	  		jobSet[i + mr.nMap] = false;
	  		jobLst.PushBack(i + mr.nMap)
	  	}
	  }
	  finished := 0
	  for finished != jobLen {
	  	index, value, _ := reflect.Select(selCase)
	  	if value.Kind() != reflect.String {
	  		 v := int(value.Int());
	  		if v < 0 {			
		  		fmt.Printf("reinsert the neg value %v \n" , v)
		  		selCase[index].Chan = reflect.ValueOf(nil)
		  		fmt.Printf("omit a select channel %v %T the index is %v\n" , value, value, index)
		  		if jobSet[(-v)] == false {
		  			jobLst.PushFront(-v)
		  		}
		  		continue
	  		} else {
	  			if v != SENDAGAIN {
	  			jobSet[v] = true
	  			finished++
	  			}
	  		}
	  	}else if index == 0 {
	  		tmp := make(chan int)
	  		selCase = append(selCase, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(tmp)})
	  		go mr.callWorker(value.String(), tmp)
	  		index = len(selCase) - 1
	  	}
	  	
	  	nextJob := -1 
	  	for e:= jobLst.Front(); e != nil; e = e.Next() {
	  		v := e.Value.(int)
	  		jobLst.Remove(e)
	  		if jobSet[v] == true {
	  			continue
	  		} else {
	  			nextJob = v
	  			break;
	  		}
	  	}
	  	if nextJob == -1 {
	  		DPrintln("no more job in current jobset")
	  		DPrintln("finished should be jobLen: " + strconv.Itoa(finished))
	  		//close the callWorker routine
			nextJob = SENDAGAIN	
	  	} else {
	  		
	  	}	
	  	selCase[index].Chan.Send(reflect.ValueOf(nextJob))
	  }
	  DPrintln("phase " + strconv.Itoa(phase) + " done")
  }	  
  DPrintf("@@@@@@@@@gonna fin go routins in RunMaster\n")
  //finish go routine
  for i, v := range selCase {
  	if i == 0 {
  		continue
  	}
  	ch := v.Chan
  	if ch != reflect.ValueOf(nil){
  		DPrintln("channel not nil")
  		channel := selCase[i].Chan
  		i, e :=channel.Recv()
  		if e == false || i.Int() < 0{
  			DPrintln("cannot recv fromt closing chan, recv:" + i.String()) 
  			continue
  		}
  		DPrintln("channel recv")
  		channel.Send(reflect.ValueOf(NOMOREJOBS))
  	} else {
  		fmt.Printf("finishing  routines omit index : %v\n", i) 
  	}
  }
  DPrintln("$$$$$$$$$before quit<-1")
  quit <- 1
  return mr.KillWorkers()
}
