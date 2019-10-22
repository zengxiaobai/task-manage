package common

import (
	"os"
	"os/signal"
	"syscall"
	"time"
	log "github.com/cihub/seelog"
)

type ExeInterface interface {
	LogInit()
	ChooseStrategy()
	ExeInit()
	RocketMQStart()
}

type ExeCommon struct {
	CfgFilename string
	RebuildFilename string
	StrategyInterface Strategies
}

func RegisterExitSignal() {
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	closeCnt := 0
	go func() {
		for {
			select {
			case <-signalChan:
				if closeCnt == 0 {
					close(ExitChan)
					closeCnt++
				} else {
					log.Info("Error ", "close ExitChan twice")
				}
			}
		}
	}()
}

func (e* ExeCommon)Run(ein ExeInterface) {
	ein.LogInit()
	ConfigInit(e.CfgFilename)
	RegisterExitSignal()
	ein.ChooseStrategy()
	ein.ExeInit()
	go RebuildTask(e.RebuildFilename)
	go e.StrategyInterface.GoRecieveMsg()
	time.Sleep(time.Duration(500) * time.Millisecond)
	ein.RocketMQStart()
}