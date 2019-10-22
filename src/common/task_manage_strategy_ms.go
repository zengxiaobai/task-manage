package common

import (
	"container/list"
	log "github.com/cihub/seelog"
	"os"
	"time"
)

type dealElemMs struct {
	dealElem
	t *taskManagerMS
}

type taskManagerMS struct {
	l              *list.List
	lIter          *list.Element
	enterpriseCode string
}
type mapManagerMs struct {
	hostMap       map[string]*taskManagerMS
	len           int
	taskDelChan   chan dealElemMs
	taskInputChan chan dealElemMs
	taskRetryChan chan dealElemMs
}

type MaxGorutinesStrategy struct {
	dealMsgInterface taskDealMsg
	rebuildFilename  string
	maxGorutines     int
	dealInterval     int
	mm               mapManagerMs
}

func (ms *MaxGorutinesStrategy) mapManagerMsInit() {
	ms.mm = mapManagerMs{}
	ms.mm.hostMap = make(map[string]*taskManagerMS)
	ms.mm.len = 0
	ms.mm.taskDelChan = make(chan dealElemMs, ms.maxGorutines)
	ms.mm.taskInputChan = make(chan dealElemMs, ms.maxGorutines)
	ms.mm.taskRetryChan = make(chan dealElemMs, ms.maxGorutines)

}
func (ms *MaxGorutinesStrategy) Init(fname string, maxGorutines, dealInterval int, t taskDealMsg) {
	ms.rebuildFilename = fname
	ms.maxGorutines = maxGorutines
	ms.dealInterval = dealInterval
	ms.dealMsgInterface = t
}

func (ms *MaxGorutinesStrategy) storeTask() {
	f, fErr := os.OpenFile(ms.rebuildFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if fErr != nil {
		log.Info("Error ", fErr)
		return
	}
	defer f.Close()
	for _, v := range ms.mm.hostMap {
		storeTaskToDisk(f, v.l)
	}
}

func (ms *MaxGorutinesStrategy) goDealTask() {
	for {
		select {
		case m := <-ms.mm.taskInputChan:
			tNode := (m.e.Value).(taskNode)
			msgStruct := tNode.InputMsg
			dealResult := ms.dealMsgInterface.DealMsg(msgStruct, tNode.retryTimes)
			if dealResult == -1 {
				ms.mm.taskRetryChan <- m
			} else {
				ms.mm.taskDelChan <- m
			}
		}
	}
}

func (ms *MaxGorutinesStrategy) taskGorutinesInit() {
	cnt := 0
	for cnt < ms.maxGorutines {
		go ms.goDealTask()
		cnt++
	}
}

func (ms *MaxGorutinesStrategy) addTaskListElem(t *taskManagerMS, node taskNode) {
	e := t.l.PushBack(node)
	if t.lIter == nil {
		t.lIter = e
	}
}

func (ms *MaxGorutinesStrategy) mvBackTaskListElem(delT dealElemMs) {
	node := delT.l.Remove(delT.e).(taskNode)
	node.retryTimes++
	tail := delT.l.PushBack(node)
	if node.retryTimes%50 == 0 {
		log.Info("WARN: task node retry many times ", node)
	}
	if delT.t.lIter == nil {
		delT.t.lIter = tail
	}
}

func (ms *MaxGorutinesStrategy) doTask(inputT dealElemMs) {
	for {
		select {
		case ms.mm.taskInputChan <- inputT:
			return
		case delT := <-ms.mm.taskDelChan:
			//log.Info("remove list len before ", delT.l.Len(), *delT.e)
			delT.l.Remove(delT.e)
			//log.Info("remove list len after ", delT.l.Len(), *delT.e)
		case retryT := <-ms.mm.taskRetryChan:
			ms.mvBackTaskListElem(retryT)
		}
	}
}

func (ms *MaxGorutinesStrategy) iterTask() {
	for k, v := range ms.mm.hostMap {
		if v.l.Len() > 0 {
			if v.lIter != nil {
				inputT := dealElemMs{
					dealElem: dealElem{l: v.l, e: v.lIter},
					t:        v,
				}
				ms.doTask(inputT)
				log.Info("NOW deal ", *inputT.e)
				v.lIter = v.lIter.Next()
			}
		} else {
			delete(ms.mm.hostMap, k)
			log.Info("delete host ", k, " from map\n")
		}
	}
}

func (ms *MaxGorutinesStrategy) addTaskMap(k string) *taskManagerMS {
	var t = taskManagerMS{}
	t.l = list.New()
	t.enterpriseCode = k
	t.lIter = t.l.Front()
	ms.mm.hostMap[k] = &t
	ms.mm.len++
	return &t
}

func (ms *MaxGorutinesStrategy) GoRecieveMsg() {
	ms.mapManagerMsInit()
	ms.taskGorutinesInit()
	ticker := time.NewTicker(time.Millisecond * time.Duration(ms.dealInterval))
	exitCnt := 0
	for {
		select {
		case <-ExitChan:
			log.Info("exitCnt ", exitCnt)
			exitCnt++
			time.Sleep(time.Second * time.Duration(1))
			log.Info("exitCnt after sleep ", exitCnt)
			if exitCnt > 4 {
				ms.storeTask()
				log.Info("Exit store Done!")
				os.Exit(1)
			}
		case msgStruct := <-MsgChan:
			v, ok := ms.mm.hostMap[msgStruct.EnterpriseCode]
			if !ok {
				v = ms.addTaskMap(msgStruct.EnterpriseCode)
			}
			ms.addTaskListElem(v, taskNode{retryTimes: 0, InputMsg: msgStruct})
		case <-ticker.C:
			ms.iterTask()
		case delT := <-ms.mm.taskDelChan:
			log.Info("remove list len before ", delT.l.Len(), *delT.e)
			delT.l.Remove(delT.e)
			log.Info("remove list len after ", delT.l.Len(), *delT.e)
		case retryT := <-ms.mm.taskRetryChan:
			ms.mvBackTaskListElem(retryT)
		}
	}
}
