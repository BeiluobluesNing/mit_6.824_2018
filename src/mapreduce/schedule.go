package mapreduce

import "fmt"

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
// use to give each worker a sequence of task
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	contaskargs := func(phase jobPhase, task int) DoTaskArgs{
		debug("task: %d\n",task)
		var taskargs DoTaskArgs
		taskargs.Phase = phase
		taskargs.JobName = jobName
		taskargs.NumOtherPhase = n_other
		taskargs.TaskNumber = task
		if phase == mapPhase{
			taskargs.File = mapFiles[task]
		}
		return taskargs
	}
	//创建一个channel用于进程间的通信
	tasks := make(chan int)
	go func(){
		for i:= 0;i<ntasks;i++{
			//向channel 中传送数据
			tasks <- i
		}
	}()
	suctasks := 0
	suc := make(chan int)
	/*
	 可以不用建议的sync.Waitgroup...
	 可以让worker在完成一个task的时候返回一个信息
	 master将会继续查询完成的tasks数
	 当所有task完成，master取消该循环并结束调度
	 */
	loop:
		for{
			select {
			//从tasks中读取数据
			case task := <-tasks:
				go func(){
					//从registerChan中读取
					worker := <- registerChan
					status := call(worker,"Worker.DoTask",contaskargs(phase,task),nil)
					if status{//worker成功，将起加入registerChan
						suc <- 1
						go func(){ registerChan <- worker}()
					} else{//woker 执行失败，将task返回taskchan
						tasks <- task
					}
				}()
			case <- suc:
				suctasks +=1
			default:
				if suctasks == ntasks{
					break loop
				}
			}
		}

	fmt.Printf("Schedule: %v done\n", phase)
}
