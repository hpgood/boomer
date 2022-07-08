package boomer

import (
	"fmt"
	"log"
	"math/rand"
	"os"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	stateInit     = "ready"
	stateSpawning = "spawning"
	stateRunning  = "running"
	stateStopped  = "stopped"
	stateQuitting = "quitting"
)

const (
	slaveReportInterval = 3 * time.Second
	heartbeatInterval   = 1 * time.Second
)

type runner struct {
	state string

	tasks           []*Task
	totalTaskWeight int

	rateLimiter      RateLimiter
	rateLimitEnabled bool
	stats            *requestStats

	numClients int32
	spawnRate  float64
	host       string

	// all running workers(goroutines) will select on this channel.
	// close this channel will stop all running workers.
	stopChan chan bool

	// close this channel will stop all goroutines used in runner.
	closeChan chan bool

	outputs []Output
	seq     int
}

// safeRun runs fn and recovers from unexpected panics.
// it prevents panics from Task.Fn crashing boomer.
func (r *runner) safeRun(fn func(ctx *RunContext), ctx *RunContext) {
	defer func() {
		// don't panic
		err := recover()
		if err != nil {
			stackTrace := debug.Stack()
			errMsg := fmt.Sprintf("%v", err)
			os.Stderr.Write([]byte(errMsg))
			os.Stderr.Write([]byte("\n"))
			os.Stderr.Write(stackTrace)
		}
	}()
	fn(ctx)
}

func (r *runner) addOutput(o Output) {
	r.outputs = append(r.outputs, o)
}

func (r *runner) outputOnStart() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStart()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnEevent(data map[string]interface{}) {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnEvent(data)
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) outputOnStop() {
	size := len(r.outputs)
	if size == 0 {
		return
	}
	wg := sync.WaitGroup{}
	wg.Add(size)
	for _, output := range r.outputs {
		go func(o Output) {
			o.OnStop()
			wg.Done()
		}(output)
	}
	wg.Wait()
}

func (r *runner) spawnWorkers(baseCount int, spawnCount int, quit chan bool, host string, spawnCompleteFunc func()) {
	log.Println("Spawning", spawnCount, "clients at the rate", r.spawnRate, "clients/s...", "host", host)

	defer func() {
		if spawnCompleteFunc != nil {
			spawnCompleteFunc()
		}
	}()
	//baseCount =1
	startI := baseCount + 1
	endI := baseCount + spawnCount

	// log.Println("runner@spawnWorkers spawnCount=", spawnCount, " baseCount=", baseCount, " startI=", startI, " endI=", endI)

	for i := startI; i <= endI; i++ { // workers

		if i > startI {
			sleepTime := time.Duration(1000000/r.spawnRate) * time.Microsecond
			time.Sleep(sleepTime)
		}

		select {
		case <-quit:
			// quit spawning goroutine
			return
		default:
			atomic.AddInt32(&r.numClients, 1)

			// log.Println("@spawnWorkers r.numClients=", r.numClients)
			//context
			ctx := NewRunContext()
			ctx.ID = i
			ctx.RunSeq = 1
			ctx.RunHost = host

			// log.Println("@spawnWorkers ctx.ID=", i)

			if len(r.tasks) == 0 {
				log.Println("@spawnWorkers Error: no task to run !")
				continue
			}
			loopMode := false
			var loopTask *Task

			go func() {
				seq := -1
				for {
					select {
					case <-quit:
						return
					default:
						pass := true
						if r.rateLimitEnabled {
							blocked := r.rateLimiter.Acquire()
							if blocked {
								pass = false
							}
						}

						if pass {
							if loopMode {
								if ctx.TaskLoop && loopTask != nil {
									ctx.TaskLoopID = ctx.TaskLoopID + 1
									task := loopTask
									r.safeRun(task.Fn, ctx)
									ctx.RunSeq++
								} else {
									ctx.TaskLoop = false
									loopMode = false
								}
							} else {
								if runRandom {
									seq = r.getRandomTaskIdx()
								} else {
									seq = r.nextSeq(seq)
								}
								task := r.getTaskBySeq(seq)
								ctx.TaskLoopID = 1
								ctx.TaskLoop = false
								r.safeRun(task.Fn, ctx)
								//是否循环执行
								loopMode = ctx.TaskLoop
								if ctx.TaskLoop {
									loopTask = task
								} else {
									loopTask = nil
								}
								ctx.RunSeq++
							}
						}
					}
				}
			}()
		}
	}
}

// setTasks will set the runner's task list AND the total task weight
// which is used to get a random task later
func (r *runner) setTasks(t []*Task) {
	r.tasks = t
	r.seq = 0
	weightSum := 0
	for _, task := range r.tasks {
		weightSum += task.Weight
	}
	r.totalTaskWeight = weightSum
}
func (r *runner) nextSeq(seq int) int {
	seq++
	if seq < 0 {
		seq = 0
	}
	if seq >= len(r.tasks) {
		seq = 0
	}

	return seq
}
func (r *runner) getTaskBySeq(seq int) *Task {
	if seq >= len(r.tasks) {
		seq = 0
	}
	return r.tasks[seq]
}
func (r *runner) getTask() *Task {
	tasksCount := len(r.tasks)
	if tasksCount == 1 {
		// Fast path
		return r.tasks[0]
	}

	if !runRandom {
		if r.seq >= len(r.tasks) {
			r.seq = 0
		}
		t := r.tasks[r.seq]
		r.seq++
		if r.seq >= len(r.tasks) {
			r.seq = 0
		}
		return t
	}

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	totalWeight := r.totalTaskWeight
	if totalWeight <= 0 {
		// If all the tasks have not weights defined, they have the same chance to run
		randNum := rs.Intn(tasksCount)
		return r.tasks[randNum]
	}

	randNum := rs.Intn(totalWeight)
	runningSum := 0
	for _, task := range r.tasks {
		runningSum += task.Weight
		if runningSum > randNum {
			return task
		}
	}

	return nil
}
func (r *runner) getRandomTaskIdx() int {
	tasksCount := len(r.tasks)
	if tasksCount == 1 {
		// Fast path
		return 0
	}

	rs := rand.New(rand.NewSource(time.Now().UnixNano()))

	totalWeight := r.totalTaskWeight
	if totalWeight <= 0 {
		// If all the tasks have not weights defined, they have the same chance to run
		randNum := rs.Intn(tasksCount)
		return randNum
	}
	randNum := rs.Intn(totalWeight)
	runningSum := 0
	for idx, task := range r.tasks {
		runningSum += task.Weight
		if runningSum > randNum {
			return idx
		}
	}
	return 0
}

func (r *runner) startSpawning(baseCount int, userClassesCount map[string]int64, spawnCount int, spawnRate float64, host string, spawnCompleteFunc func()) {
	Events.Publish("boomer:hatch", spawnCount, spawnRate)
	Events.Publish("boomer:spawn", spawnCount, spawnRate)

	if r.numClients == 0 {
		r.stats.clearStatsChan <- true
		r.stopChan = make(chan bool)
	}

	r.spawnRate = spawnRate
	// r.numClients = 0
	r.host = host
	r.stats.userClassesCount = userClassesCount

	go r.spawnWorkers(baseCount, spawnCount, r.stopChan, host, spawnCompleteFunc)
}

func (r *runner) stop() {
	log.Println("runner@stop")
	// publish the boomer stop event
	// user's code can subscribe to this event and do thins like cleaning up
	Events.Publish("boomer:stop")
	r.numClients = 0
	// stop previous goroutines without blocking
	// those goroutines will exit when r.safeRun returns
	if r.stopChan == nil {
		log.Println("@stop Error: no chan!")
	} else {
		close(r.stopChan)
	}

	if r.stats != nil {
		for k := range r.stats.userClassesCount {
			delete(r.stats.userClassesCount, k)
		}
	}

	if r.rateLimitEnabled {
		r.rateLimiter.Stop()
	}
}

type localRunner struct {
	runner

	spawnCount int
}

func newLocalRunner(tasks []*Task, rateLimiter RateLimiter, spawnCount int, spawnRate float64, host string) (r *localRunner) {
	r = &localRunner{}
	r.setTasks(tasks)
	r.spawnRate = spawnRate
	r.spawnCount = spawnCount
	r.host = host
	r.closeChan = make(chan bool)
	r.addOutput(NewConsoleOutput())

	if rateLimiter != nil {
		r.rateLimitEnabled = true
		r.rateLimiter = rateLimiter
	}

	r.stats = newRequestStats()
	return r
}

func (r *localRunner) run() {
	r.state = stateInit
	r.stats.start()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				data["user_count"] = r.numClients
				r.outputOnEevent(data)
			case <-r.closeChan:
				log.Println("@run rev stop")
				Events.Publish("boomer:quit")
				r.stop()
				wg.Done()
				return
			}
		}
	}()

	if r.rateLimitEnabled {
		r.rateLimiter.Start()
	}
	r.startSpawning(0, r.stats.userClassesCount, r.spawnCount, r.spawnRate, r.host, nil)

	wg.Wait()
}

func (r *localRunner) close() {
	if r.stats != nil {
		r.stats.close()
	}
	close(r.closeChan)
}

// SlaveRunner connects to the master, spawns goroutines and collects stats.
type slaveRunner struct {
	runner

	nodeID     string
	masterHost string
	masterPort int
	client     client
}

func newSlaveRunner(masterHost string, masterPort int, tasks []*Task, rateLimiter RateLimiter) (r *slaveRunner) {
	r = &slaveRunner{}
	r.masterHost = masterHost
	r.masterPort = masterPort
	r.setTasks(tasks)
	r.nodeID = getNodeID()
	r.closeChan = make(chan bool)

	if rateLimiter != nil {
		r.rateLimitEnabled = true
		r.rateLimiter = rateLimiter
	}

	r.stats = newRequestStats()
	return r
}

func (r *slaveRunner) spawnComplete() {
	// log.Println("@spawnComplete r.numClients=", r.numClients)
	data := make(map[string]interface{})
	data["count"] = r.numClients
	data["user_classes_count"] = r.stats.userClassesCount
	r.client.sendChannel() <- newMessage("spawning_complete", data, r.nodeID)
	r.state = stateRunning
}

func (r *slaveRunner) onQuiting() {
	if r.state != stateQuitting {
		r.client.sendChannel() <- newMessage("quit", nil, r.nodeID)
	}
}

func (r *slaveRunner) close() {
	if r.stats != nil {
		r.stats.close()
	}
	if r.client != nil {
		r.client.close()
	}
	close(r.closeChan)
}

//toInt64 自动转换int64
func toInt64(t interface{}) int64 {
	if _, ok := t.(uint64); ok {
		return int64(t.(uint64))
	} else {
		if _, ok := t.(int64); ok {
			return t.(int64)
		}
		return int64(t.(int))
	}
}
func (r *slaveRunner) onSpawnMessage(msg *message) {
	r.client.sendChannel() <- newMessage("spawning", nil, r.nodeID)
	rate, hasRate := msg.Data["spawn_rate"]
	users, hasUsers := msg.Data["num_users"]
	host := ""
	if _host, ok := msg.Data["host"]; ok {

		if _host != nil {
			arr := _host.([]uint8)
			if len(arr) > 0 {
				host = string(arr)
			}
		}

	}

	// log.Println("@onSpawnMessage msg=", msg)
	// log.Println("@onSpawnMessage msg.Data=", msg.Data)
	// [host:[116] stop_timeout:<nil> timestamp:1.640676259932507e+09 user_classes_count:map[QuickstartUser:0]
	spawnRate := 1.0
	classCount := make(map[string]int64)

	var curCount int64 = 0

	workers := 1
	if !hasUsers {
		// log.Println("@onSpawnMessage Warn: msg.Data['num_users'] is none !")
	} else {
		workers = int(toInt64(users))
	}

	baseCount := 0

	if !hasRate {
		// log.Println("@onSpawnMessage  locust version >2")
		// for k, v := range msg.Data {
		// 	if k == "parsed_options" {
		// 		// for k2, v2 := range v.(map[interface{}]interface{}) {
		// 		// 	log.Println("@onSpawnMessage parsed_options[", k2.(string), "]=", v2)
		// 		// }
		// 	} else {
		// 		log.Println("@onSpawnMessage key:", k, "=", v)
		// 	}
		// }
		uc, ok := msg.Data["user_classes_count"]
		if !ok {
			log.Println("@onSpawnMessage  Error: msg.Data['user_classes_count'] = nil ")
			return
		}

		_classCount := uc.(map[interface{}]interface{})
		check := false
		for k, v := range _classCount {
			num := toInt64(v)
			key := k.(string)
			curCount = num
			classCount[key] = num
			if !check {
				if n, ok := r.stats.userClassesCount[key]; ok {
					baseCount = int(n)
					curCount = num - n
				} else {
					curCount = num
				}
				check = true
			}

		}
		log.Println("runner@onSpawnMessage user_classes_count:", classCount)
	} else {
		spawnRate = rate.(float64)
	}
	if curCount > 0 {
		workers = int(curCount)
		if workers <= 100 {
			spawnRate = float64(workers)
		} else {
			spawnRate = 100
			if workers > 1000 {
				spawnRate = 1000
			}
		}
	}
	// log.Println("@onSpawnMessage workers=", workers)
	// log.Println("@onSpawnMessage spawnRate=", spawnRate)
	if r.rateLimitEnabled {
		r.rateLimiter.Start()
	}
	r.startSpawning(baseCount, classCount, workers, spawnRate, host, r.spawnComplete)
}

// Runner acts as a state machine.
func (r *slaveRunner) onMessage(msg *message) {
	if msg.Type == "hatch" {
		log.Println("The master sent a 'hatch' message, you are using an unsupported locust version, please update locust to 1.2.")
		return
	}

	switch r.state {
	case stateInit:
		switch msg.Type {
		case "spawn":
			r.state = stateSpawning
			r.onSpawnMessage(msg)
		case "quit":
			Events.Publish("boomer:quit")
		}
	case stateSpawning:
		fallthrough
	case stateRunning:
		switch msg.Type {
		case "spawn":
			// log.Println("@onMessage spawn ,stop")
			r.state = stateSpawning
			// r.stop()
			r.onSpawnMessage(msg)
		case "stop":
			r.stop()
			r.state = stateStopped
			log.Println("Recv stop message from master, all the goroutines are stopped")
			r.client.sendChannel() <- newMessage("client_stopped", nil, r.nodeID)
			r.client.sendChannel() <- newMessage("client_ready", nil, r.nodeID)
			r.state = stateInit
		case "quit":
			r.stop()
			log.Println("Recv quit message from master, all the goroutines are stopped")
			Events.Publish("boomer:quit")
			r.state = stateInit
		}
	case stateStopped:
		switch msg.Type {
		case "spawn":
			r.state = stateSpawning
			r.onSpawnMessage(msg)
		case "quit":
			Events.Publish("boomer:quit")
			r.state = stateInit
		}
	}
}

func (r *slaveRunner) startListener() {
	go func() {
		for {
			select {
			case msg := <-r.client.recvChannel():
				r.onMessage(msg)
			case <-r.closeChan:
				return
			}
		}
	}()
}

func (r *slaveRunner) run() {
	r.state = stateInit
	r.client = newClient(r.masterHost, r.masterPort, r.nodeID)

	err := r.client.connect()
	if err != nil {
		if strings.Contains(err.Error(), "Socket type DEALER is not compatible with PULL") {
			log.Println("Newer version of locust changes ZMQ socket to DEALER and ROUTER, you should update your locust version.")
		} else {
			log.Printf("Failed to connect to master(%s:%d) with error %v\n", r.masterHost, r.masterPort, err)
		}
		return
	}

	// listen to master
	r.startListener()

	r.stats.start()

	// tell master, I'm ready

	r.client.sendChannel() <- newMessage("client_ready", nil, r.nodeID)

	// report to master
	go func() {
		for {
			select {
			case data := <-r.stats.messageToRunnerChan:
				if r.state == stateInit || r.state == stateStopped {
					continue
				}
				data["user_count"] = r.numClients
				r.client.sendChannel() <- newMessage("stats", data, r.nodeID)
				r.outputOnEevent(data)
			case <-r.closeChan:
				return
			}
		}
	}()

	// heartbeat
	// See: https://github.com/locustio/locust/commit/a8c0d7d8c588f3980303358298870f2ea394ab93
	go func() {
		var ticker = time.NewTicker(heartbeatInterval)
		for {
			select {
			case <-ticker.C:
				CPUUsage := GetCurrentCPUUsage()
				data := map[string]interface{}{
					"state":             r.state,
					"current_cpu_usage": CPUUsage,
				}
				r.client.sendChannel() <- newMessage("heartbeat", data, r.nodeID)
			case <-r.closeChan:
				return
			}
		}
	}()

	Events.Subscribe("boomer:quit", r.onQuiting)
}
