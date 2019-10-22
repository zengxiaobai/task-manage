package rocketmqMy

import (
	"encoding/json"
	"fmt"
	rocketmq "github.com/apache/rocketmq-client-go/core"
	log "github.com/cihub/seelog"
	"net/url"
	"rocketmq-filemgr/src/common"
//	"time"
)

func GenPcConfig(NameServer, GroupID string, Model rocketmq.MessageModel) (config *rocketmq.PushConsumerConfig) {
	pcConfig := rocketmq.PushConsumerConfig{}
	pcConfig.GroupID = GroupID
	pcConfig.NameServer = NameServer
	pcConfig.LogC = &rocketmq.LogConfig{
		Path:     "/tmp/log",
		FileSize: 64 * 1 << 10,
		FileNum:  1,
		Level:    rocketmq.LogLevelDebug,
	}
	/* 广播模式下不能reconsumerlater 所以使用广播模式时 同时修改使用策略ms */
	pcConfig.Model = Model
	credit := rocketmq.SessionCredentials{AccessKey: "squidcontent", SecretKey: "hotwon!2016", Channel: ""}
	pcConfig.Credentials = &credit
	pcConfig.MaxReconsumeTimes = 240
	return &pcConfig
}

func ConsumeWithPush(config *rocketmq.PushConsumerConfig, topic string, exitChan chan struct{}, msgChan chan common.InputMsg) {

	consumer, err := rocketmq.NewPushConsumer(config)
	if err != nil {
		println("create Consumer failed, error:", err)
		return
	}

	// MUST subscribe topic before consumer started.
	consumer.Subscribe(topic, "*", func(msg *rocketmq.MessageExt) rocketmq.ConsumeStatus {
//        fmt.Println("Reconsume times 1", msg.GetMessageReconsumeTimes())
//        fmt.Println("Delay time level 1", msg.GetMessageDelayTimeLevel())

		/* cannot use exitChan because can read only onece */
		select {
		case <-exitChan:
			log.Info("Consumer Recieve exit ", msg.Body)
			msg.SetMessageDelayTimeLevel(4)
			return rocketmq.ReConsumeLater
		default:
			msgStruct := common.InputMsg{}
			if jsonErr := json.Unmarshal([]byte(msg.Body), &msgStruct); jsonErr != nil {
				log.Info("ERROR: ", jsonErr, "input ", msg.Body)
				return rocketmq.ConsumeSuccess
			}
			log.Info(msgStruct.UrlMd5)
			if _, uErr := url.Parse(msgStruct.ContentUrl); uErr != nil {
				log.Info("ERROR: msgStruct.ContentUr ",msgStruct.ContentUrl, " ", uErr)
				return rocketmq.ConsumeSuccess
			}
			if msgStruct.EnterpriseCode == "" {
				msgStruct.EnterpriseCode = "other"
			}
			if v, ok := common.MsgFeedbackMap.Load(msgStruct.EnterpriseCode); ok {
				if v == 1 {
					if msg.GetMessageReconsumeTimes() > 5 {
						msg.SetMessageDelayTimeLevel(5)
					}
					//fmt.Println(time.Now().Unix(), " ReConsumeLater TEST A message received:  \n", msg.Body)
					log.Info("Reconsume times ", msg.GetMessageReconsumeTimes(), " Delay time level ",  msg.GetMessageDelayTimeLevel())
					return rocketmq.ReConsumeLater
				}
			}
			msgChan <- msgStruct
			return rocketmq.ConsumeSuccess
		}
	})

	err = consumer.Start()
	if err != nil {
		println("consumer start failed,", err)
		return
	}

	fmt.Printf("consumer: %s started...\n", consumer)
	select {
	case <-exitChan:
		err = consumer.Shutdown()
		fmt.Printf("consumer shutdowning")
		if err != nil {
			fmt.Printf("consumer shutdown failed")
		}
		return
	}
}
