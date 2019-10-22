package common

import (
	"container/list"
	"os"
	"sync"
	"time"
	"xcloud/mynet"
	log "github.com/cihub/seelog"
)

type HostGorutinesStrategy struct {
	DealMsgInterface taskDealMsg
	RebuildFilename string
}

type taskManager struct {
	ic        chan InputMsg
	msgDelCh  chan *list.Element
	msgCh     chan *list.Element
	childNums int
	l         *list.List
	lIter     *list.Element
	host      string
}
type mapManager struct {
	hostMap map[string]*taskManager
	len     int
}

var (
	taskExitChan chan struct{}
	/* if not make it will always block*/
	taskExitCycleChan chan int
	syncMap sync.Map
	mm      mapManager
)

func (hs * HostGorutinesStrategy)StoreTask() {
	f, fErr := os.OpenFile(hs.RebuildFilename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if fErr != nil {
		log.Info("Error ", fErr)
		return
	}
	defer f.Close()
	for _, v := range mm.hostMap {
		storeTaskToDisk(f, v.l)
	}
}

func (hs *HostGorutinesStrategy) GoRecieveMsg() {
	exitCnt := 0
	for {
		select {
		case <-ExitChan:
			/* waitFor ConsumePush read exitChan */
			log.Info("exitCnt ", exitCnt)
			exitCnt++
			time.Sleep(time.Second * time.Duration(1))
			log.Info("exitCnt after sleep ", exitCnt)
			if exitCnt > 3 {
				log.Info("closing taskExitChan ", mm.len)
				close(taskExitChan)
				cnt := 0
				t1 := time.NewTimer(time.Second * time.Duration(1))
				defer t1.Stop()
				for {
					select {
					case <-taskExitCycleChan:
						cnt++
						if cnt == mm.len {
							hs.StoreTask()
							log.Info("All taskManager exit")
							log.Flush()
							os.Exit(0)
						}
					case <-t1.C:
						hs.StoreTask()
						log.Info("ERROR exit timeout")
						log.Flush()
						os.Exit(-1)
					}
				}
			}
		case msgStruct := <-MsgChan:
			if exitCnt > 0 {
				log.Info("ERROR: this is just for test exit", exitCnt)
				exitCnt = 0
			}
			_, h, _, _, hErr := mynet.Get_proto_host_port_urlpath(msgStruct.ContentUrl)
			if hErr != nil || h == "" {
				log.Info("ERROR: msgStruct Get_proto_host_port_urlpath ERROR ", msgStruct)
				break
			}
			v, ok := mm.hostMap[h]
			if !ok {
				var t = taskManager{}
				t.ic = make(chan InputMsg)
				t.msgDelCh = make(chan *list.Element, PushCfg.PushMaxHostGorutines)
				t.msgCh = make(chan *list.Element, PushCfg.PushMaxHostGorutines)
				t.l = list.New()
				t.lIter = t.l.Front()
				mm.hostMap[h] = &t
				v = &t
				mm.len++
				go hs.goManageTask(&t)
			}
			v.ic <- msgStruct
		}
	}
}
func (hs *HostGorutinesStrategy) goManageTask(t *taskManager) {
	if t.childNums < 1 {
		i := 0
		for i < PushCfg.PushMaxHostGorutines {
			go hs.goDealTask(t)
			i++
		}
		t.childNums = i
	}
	ticker := time.NewTicker(time.Millisecond * time.Duration(PushCfg.PushListIterTicker))
	exitCnt := 0
	for {
		select {
		case msgStruct := <-t.ic:
			if exitCnt > 0 {
				log.Info("ERROR: this is just for test exit 2", exitCnt)
				exitCnt = 0
			}
			if t.l.Len() > PushCfg.PushMaxListLen {
				log.Info("ERROR: drop because list len ", t.l.Len(), " > ", PushCfg.PushMaxListLen, msgStruct)

			} else {
				e := t.l.PushBack(msgStruct)
				if t.lIter == nil {
					t.lIter = e
				}
				hs.iterTask(t)
				log.Info("push list len ", t.l.Len(), " push msgStruct ", msgStruct)
			}
		case m := <-t.msgDelCh:
			log.Info("remove list len before ", t.l.Len(), m)
			t.l.Remove(m)
			hs.iterTask(t)
			log.Info("remove list len after ", t.l.Len())

		case <-ticker.C:
			//log.Info("ticker ", t.lIter, "len ", t.l.Len())
			hs.iterTask(t)
		case <-taskExitChan:
			log.Info("Recieve taskExitChan", exitCnt)
			exitCnt++
			time.Sleep(time.Second * time.Duration(1))
			if exitCnt > 3 {
				taskExitCycleChan <- 1
				log.Info("TaskManager ", *t, " EXIT")
				return
			}
		}
	}
}
func (hs *HostGorutinesStrategy) iterTask(t *taskManager) {
	for i := t.lIter; i != nil; {
		select {
		case t.msgCh <- i:
			log.Info("NOW deal ", i)
			t.lIter = t.lIter.Next()
			i = t.lIter
		default:
			return
		}
	}
}

func (hs *HostGorutinesStrategy) goDealTask(t *taskManager) {
	for {
		select {
		case m := <-t.msgCh:
			msgStruct := (m.Value).(InputMsg)
			hs.DealMsgInterface.DealMsg(msgStruct, 0)
			t.msgDelCh <- m
		}
	}
}
