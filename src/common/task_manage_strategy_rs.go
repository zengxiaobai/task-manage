package common

import (
	"container/list"
	log "github.com/cihub/seelog"
	"os"
	"time"
)

type dealElemRs struct {
	dealElem
	t *taskManagerRS
}

type taskManagerRS struct {
	l *list.List
	/* if we add task node such as push back or move back, modify it */
	lIter *list.Element
	/* sync changes with leftGorutines */
	dealingCnt     int
	enterpriseCode string
}
type mapManagerRs struct {
	hostMap map[string]*taskManagerRS
	len     int
	/* sync changes with dealingCnt */
	leftGorutines int
	taskDelChan   chan dealElemRs
	taskRetryChan chan dealElemRs
	taskInputChan chan dealElemRs
}

type ReconsumerStrategy struct {
	dealMsgInterface taskDealMsg
	rebuildFilename  string
	maxGorutines     int
	dealInterval     int
	reconsumeListLen int
	mm               mapManagerRs
}

func (rs *ReconsumerStrategy) mapManagerRsInit() {
	rs.mm = mapManagerRs{}
	rs.mm.hostMap = make(map[string]*taskManagerRS)
	rs.mm.len = 0
	rs.mm.taskDelChan = make(chan dealElemRs, rs.maxGorutines)
	rs.mm.taskInputChan = make(chan dealElemRs, rs.maxGorutines)
	rs.mm.taskRetryChan = make(chan dealElemRs, rs.maxGorutines)
}

func (rs *ReconsumerStrategy) Init(fname string, maxGorutines, dealInterval, reconsumeListLen int, t taskDealMsg) {
	rs.rebuildFilename = fname
	rs.maxGorutines = maxGorutines
	rs.dealInterval = dealInterval
	rs.reconsumeListLen = reconsumeListLen
	rs.dealMsgInterface = t
}

func (rs *ReconsumerStrategy) storeTask() {
	f, fErr := os.OpenFile(rs.rebuildFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if fErr != nil {
		log.Info("Error ", fErr)
		return
	}
	defer f.Close()
	for _, v := range rs.mm.hostMap {
		storeTaskToDisk(f, v.l)
	}
}

func (rs *ReconsumerStrategy) goDealTask() {
	for {
		select {
		case m := <-rs.mm.taskInputChan:

			tNode := (m.e.Value).(taskNode)
			/* Implement at rocketxclean and rocketxfetch */

			dealResult := rs.dealMsgInterface.DealMsg(tNode.InputMsg, tNode.retryTimes)
			if dealResult == -1 {
				rs.mm.taskRetryChan <- m
			} else {
				rs.mm.taskDelChan <- m
			}
		}
	}
}

func (rs *ReconsumerStrategy) taskGorutinesInit() {
	cnt := 0
	for cnt < rs.maxGorutines {
		go rs.goDealTask()
		cnt++
	}
	rs.mm.leftGorutines = cnt
}

func (rs *ReconsumerStrategy) removeTaskListElem(delT dealElemRs) {
	//	log.Info("remove list len before ", delT.l.Len(), *delT.e)
	delT.l.Remove(delT.e)
	delT.t.dealingCnt--
	rs.mm.leftGorutines++
	if delT.t.l.Len() < rs.reconsumeListLen {
		MsgFeedbackMap.Store(delT.t.enterpriseCode, 0)
	}
	log.Info("remove list len after ", delT.l.Len(), *delT.e, "dealing ", delT.t.dealingCnt)
}

func (rs *ReconsumerStrategy) mvBackTaskListElem(delT dealElemRs) {
	/* we cannot change old node's retry time so  use remove old and  push a new struct */
	//	delT.l.MoveToBack(delT.e)
	node := delT.l.Remove(delT.e).(taskNode)
	node.retryTimes++
	tail := delT.l.PushBack(node)
	delT.t.dealingCnt--
	rs.mm.leftGorutines++
	if node.retryTimes%50 == 0 {
		log.Info("WARN: task node retry many times ", node)
	}
	if delT.t.lIter == nil {
		delT.t.lIter = tail
	}
	log.Info("retry list len after ", delT.l.Len(), *delT.e, "dealing ", delT.t.dealingCnt)
}

func (rs *ReconsumerStrategy) addTaskListElem(t *taskManagerRS, node taskNode) {
	e := t.l.PushBack(node)
	if t.lIter == nil {
		t.lIter = e
	}
	if t.l.Len() > rs.reconsumeListLen {
		log.Info("WARN: host task list too much ", t.enterpriseCode, " len ", t.l.Len())
		MsgFeedbackMap.Store(t.enterpriseCode, 1)
	}
}

func (rs *ReconsumerStrategy) addTaskMap(k string) *taskManagerRS {
	var t = taskManagerRS{}
	t.l = list.New()
	t.enterpriseCode = k
	t.lIter = t.l.Front()
	rs.mm.hostMap[k] = &t
	rs.mm.len++
	return &t
}

func (rs *ReconsumerStrategy) removeTaskMap(k string) {
	delete(rs.mm.hostMap, k)
	rs.mm.len--
	log.Info("delete host ", k, " from map len is", rs.mm.len)
}

func (rs *ReconsumerStrategy) doTask(v *taskManagerRS, inputT dealElemRs) {
	for {
		select {
		case rs.mm.taskInputChan <- inputT:
			v.dealingCnt++
			rs.mm.leftGorutines--

			log.Info("dealingCnt ", v.dealingCnt, " leftGorutines ", rs.mm.leftGorutines)
			return
			/* to deal deadlock case
			 * if iterTask() block for  waiting gorutines which can deal taskInputChan
			 * but gorutines wait for case <- taskDelChan
			 */
		case delT := <-rs.mm.taskDelChan:
			rs.removeTaskListElem(delT)
			log.Info("dealingCnt ", v.dealingCnt, " leftGorutines ", rs.mm.leftGorutines)
		case retryT := <-rs.mm.taskRetryChan:
			rs.mvBackTaskListElem(retryT)
			log.Info("dealingCnt ", v.dealingCnt, " leftGorutines ", rs.mm.leftGorutines)
		}
	}
}

func (rs *ReconsumerStrategy) waitGorutinesForDeal() {
	log.Info("WARN: no left gorutines to deal, waiting")
	select {
	case delT := <-rs.mm.taskDelChan:
		rs.removeTaskListElem(delT)
	case retryT := <-rs.mm.taskRetryChan:
		rs.mvBackTaskListElem(retryT)
	}
	log.Info("WARN: wait one gorutines left ", rs.mm.leftGorutines)
}

func (rs *ReconsumerStrategy) iterTask() {
	iterCnt := 0
	for k, v := range rs.mm.hostMap {
		if v.l.Len() == 0 {
			rs.removeTaskMap(k)
			continue
		}
		if v.lIter == nil {
			continue
		}
		if v.dealingCnt > rs.mm.leftGorutines {
			/* avoid one host use too much gorutines */
			log.Info("WARN: ", v.enterpriseCode, "use gorutines ", v.dealingCnt, "left gorutines ", rs.mm.leftGorutines)
			continue
		}
		inputT := dealElemRs{
			dealElem: dealElem{l: v.l, e: v.lIter},
			t:        v,
		}
		rs.doTask(v, inputT)
		log.Info("NOW deal ", *inputT.e)
		v.lIter = v.lIter.Next()
		iterCnt++
	}
	if rs.mm.leftGorutines <= 0 {
		rs.waitGorutinesForDeal()
	}
}

func (rs *ReconsumerStrategy) GoRecieveMsg() {
	rs.mapManagerRsInit()
	rs.taskGorutinesInit()
	ticker := time.NewTicker(time.Millisecond * time.Duration(rs.dealInterval))
	exitCnt := 0
	for {
		select {
		case <-ExitChan:
			exitCnt++
			time.Sleep(time.Second * time.Duration(1))
			log.Info("exitCnt after sleep ", exitCnt)
			if exitCnt > 4 {
				rs.storeTask()
				log.Info("Exit store Done!")
				log.Flush()
				os.Exit(1)
			}
		case msgStruct := <-MsgChan:
			v, ok := rs.mm.hostMap[msgStruct.EnterpriseCode]
			if !ok {
				v = rs.addTaskMap(msgStruct.EnterpriseCode)
			}
			rs.addTaskListElem(v, taskNode{retryTimes: 0, InputMsg: msgStruct})
		case <-ticker.C:
			rs.iterTask()
		case delT := <-rs.mm.taskDelChan:
			rs.removeTaskListElem(delT)
		case retryT := <-rs.mm.taskRetryChan:
			rs.mvBackTaskListElem(retryT)
		}
	}
}
