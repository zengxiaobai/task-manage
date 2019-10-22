package common

import (
	"bufio"
	"container/list"
	"encoding/json"
	log "github.com/cihub/seelog"
	"os"
	"sync"
)

/* Interface Comment
 * for push and fetch, msg deal function is diffrent
 * implement it at rocketxclean and rocketxfetch
*/
type taskDealMsg interface {
	DealMsg(msgStruct InputMsg, msgDealCount int) int
}

/* Interface Comment
* task manage has two strategies:
* 1. hs, every host's task list has its own  manager and dealers,
* 2. ms, all host's task list share one common manager and common dealers
*    for every ticker, manager fetch one element from every task list and
*    push it to common dealers.
*/

type Strategies interface {
	GoRecieveMsg()
	storeTask()
}
/* 记录当前处理的task 节点， e 为节点元素； l 为e 所属的 task list */
type dealElem struct {
	l *list.List
	e *list.Element
}

type taskNode struct {
	/* if not use pointer cannot change it when node := e.Value.(taskNode)) node.retryTimes++ */
	retryTimes int
	InputMsg
}
var (
	MsgChan      chan InputMsg
	ExitChan     chan struct{}
	MsgFeedbackMap sync.Map
)
/* store task list to disk when exit */
func storeTaskToDisk(f *os.File, l *list.List) {
	for e := l.Front(); e != nil; e = e.Next() {
		tN := e.Value.(taskNode)
		msgStruct := tN.InputMsg
		b, bErr := json.Marshal(&msgStruct)
		if bErr != nil {
			log.Info("ERROR ", bErr, " ", msgStruct)
			continue
		} else {
			s := string(b)
			l := len(s)
			n, wErr := f.WriteString(string(b) + "\n")
			if wErr != nil || n != l+1 {
				log.Info("ERROR ", wErr, " ", msgStruct, "n ", n, " Len", l)
				continue
			}
			log.Info(string(b))
		}
	}
}


/* redo task list on disk when start */
func RebuildTask(rebuildFilename string) {
	f, fErr := os.Open(rebuildFilename)
	if fErr != nil {
		log.Info("Error: ", fErr)
		return
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		msgStruct := InputMsg{}
		if jsonErr := json.Unmarshal([]byte(scanner.Text()), &msgStruct); jsonErr != nil {
			log.Info("ERROR: ", jsonErr, "input ", scanner.Text())
		}
		MsgChan <- msgStruct
		log.Info(scanner.Text())
	}
	err := os.Remove(rebuildFilename)
	if err != nil {
		log.Info(err)
	}
}




func ChooseStrategy(strategy, rebuildFilename string, maxGorutines, dealInterval, reconsumeListLen int, t taskDealMsg) Strategies{
	switch strategy {
	case "rs":
		rsStrategy := ReconsumerStrategy{}
		rsStrategy.Init(rebuildFilename, maxGorutines, dealInterval, reconsumeListLen, t)
		//		rsStrategy.Init(rebuildFilename, common.PushCfg.PushMaxGorutines, common.PushCfg.PushListIterTicker, common.PushCfg.PushReconsumeListLen)
		return &rsStrategy
	case "ms":
		fallthrough
	default:
		log.Info("ERROR: strategy config is not ms and rs")
		msStrategy := MaxGorutinesStrategy{}
		msStrategy.Init(rebuildFilename, maxGorutines, dealInterval,t)
		//		msStrategy.Init(rebuildFilename,common.PushCfg.PushMaxGorutines, common.PushCfg.PushListIterTicker)
		return &msStrategy
	}
}


