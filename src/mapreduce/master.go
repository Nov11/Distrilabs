package mapreduce
import "container/list"
import "fmt"

type WorkerInfo struct {
  address string
  // You can add definitions here.
}


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

func (mr *MapReduce) acceptWorker(quit chan int){
	for true{
		select{
			case v := <-mr.registerChannel:
			mr.Workers[v] = new(WorkerInfo)
	  		mr.Workers[v].address = v
	  		case <- quit:
	  		return
		}
	}
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  //go mr.acceptWorker()
  DPrintf("@master@@@@@@@@@@@@@@@run master")
  jobToBeDone := 0
  replyChan := make([]chan int, 2) 
  for i := 0; i < 2; i++{
  	v := <-mr.registerChannel
  	mr.Workers[v] = new(WorkerInfo)
  	mr.Workers[v].address = v
  	DPrintf("@runmaster regs ", v)
  	//replyChan = append(replyChan, make(chan int))
  	replyChan[i] = make(chan int)
  	go func(c chan int){
  		for true {
	  		job := <- c
	  		DPrintf("go routine get job %v\n", job)
	  		if job == 777 {
	  			DPrintf("go routin in master fin")
	  			break;
	  		}
	  		args := &DoJobArgs{}
	  		args.File = mr.file
	  		var jobnumber int
	  		var numotherphase int
	  		var operation JobType
	  		if job < mr.nMap{
	  			jobnumber = job
	  			numotherphase = mr.nReduce
	  			operation =  Map
	  		}else{
	  			jobnumber = job - mr.nMap
	  			numotherphase = mr.nMap
	  			operation =  Reduce
	  		}
	  		args.JobNumber = jobnumber
	  		args.NumOtherPhase = numotherphase
	  		args.Operation = operation
	   	 	var reply DoJobReply
	    	ok := call(v, "Worker.DoJob", args, &reply)
		    if ok == false {
		      fmt.Printf("run master: RPC %s dojob error\n", v)
		    } else {
		      c <- 1
		    }
		}    
  	}(replyChan[i])//replyChan[len(replyChan) - 1])
  }
  //start to call worker's DoJob
  for i := range replyChan {
  	replyChan[i] <- jobToBeDone
  	jobToBeDone++
  }
  for jobToBeDone < mr.nMap + mr.nReduce {
  	select {
  		case <-replyChan[0]:
  		replyChan[0] <- jobToBeDone
  		jobToBeDone++
  		case <-replyChan[1]:
  		replyChan[1] <- jobToBeDone
  		jobToBeDone++
  	}
  }
  DPrintf("@@@@@@@@@gonna fin go routins in RunMaster")
  //finish go routine
  for i := range replyChan {
  	<-replyChan[i]
  	replyChan[i] <- 777
  }
  return mr.KillWorkers()
}
