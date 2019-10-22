package main

import (
	"encoding/json"
	"fmt"
	"github.com/apache/rocketmq-client-go/core"
	log "github.com/cihub/seelog"
	"rocketmq-filemgr/src/common"
	"rocketmq-filemgr/src/rocketmqMy"
	"runtime"
	"strconv"
	"time"
//	"net/http"
//	_ "net/http/pprof"

)


type rocketmqXclean struct {
	//Implement xclean's taskDealMsg interface
	e *common.ExeCommon
}

func (rx *rocketmqXclean)LogInit() {
	config := `
<seelog>
        <outputs formatid="main">
                <rollingfile type="size" filename="xxx" maxsize="1000000000" maxrolls="25"/>
        </outputs>
        <formats>
                <format id="main" format="%Date %Time [%File:%Line] [%Func] %Msg%n"/>
        </formats>
</seelog>
`
	logger, loggerErr := log.LoggerFromConfigAsBytes([]byte(config))
	if loggerErr != nil {
		fmt.Println(loggerErr)
	}
	log.ReplaceLogger(logger)
}

/*
 * Implement xclean's taskDealMsg interface 
 */
func (rx *rocketmqXclean) DealMsg(msg common.InputMsg, msgDealCounts int) int {
	return rockemqXcleanDealMsg(msg, msgDealCounts)
}


func (rx *rocketmqXclean)ChooseStrategy(){
	rx.e.StrategyInterface = common.ChooseStrategy(common.PushCfg.PushStrategy, rx.e.RebuildFilename, common.PushCfg.PushMaxGorutines,
		common.PushCfg.PushListIterTicker, common.PushCfg.PushReconsumeListLen, rx)
}

func (rx *rocketmqXclean)RocketMQStart() {
	//TODO model config
	xcleanPcConfig := rocketmqMy.GenPcConfig(common.CommonCfg.RocketMqNameServer, common.DeviceId + "xxx", rocketmq.Clustering)
	rocketmqMy.ConsumeWithPush(xcleanPcConfig, common.PushCfg.PushTopic, common.ExitChan, common.MsgChan)
}



func (rx *rocketmqXclean)ExeInit() {
	//TODO move it Combination Inheritance ConfigInit  RegisterExit GoRecieveMsg RebuildTask
	watchSquidPortCfg()

	/* use no buffer channel can make less error when restart */
	repChan = make(chan common.OutputMsg, 65535)
	go goMergeReport(repChan)
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	/*
	go func() {
		log.Debug(http.ListenAndServe(":6060", nil))
	}()
	*/
	var e = common.ExeCommon{
		RebuildFilename:"xxx",
		CfgFilename:"xxx",
	}
	rx := rocketmqXclean{&e}
	defer log.Flush()
	var p  common.ExeInterface = &rx
	e.Run(p)
	<- make(chan struct{})
}



/* -1 failed, 0 success */
func rockemqXcleanDealMsg(msgStruct common.InputMsg, msgDealCounts int) int {
	failed := 0
	if failed == 1 {
		return -1
	}
	return 0
}
