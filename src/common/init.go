package common

import (
	"crypto/tls"
	"syscall"

	//	"fmt"
	log "github.com/cihub/seelog"
	"net"
	"net/http"
	"os"
	"time"
	"xcloud/myfile"
	"xcloud/mynet"
)

var Trans *http.Transport

func transInit() {
	Trans = &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ResponseHeaderTimeout: time.Second * 20,
		DisableKeepAlives:     false,
		MaxIdleConns:          9000,
		MaxIdleConnsPerHost:   1000,
		DisableCompression:    true,
		IdleConnTimeout:       time.Duration(900) * time.Second,
		TLSClientConfig:       &tls.Config{InsecureSkipVerify: true},
	}
}


func rlimitInit() {

}

func machineIpInit() error {
	var err error
	MachineIp, err = mynet.MachineIp()
	return err
}
func deviceIdInit() error {
	var err error
	DeviceId, err = mynet.DeviceId()
	return err
}

func dirInit() {
	myfile.Statdir_and_mkdir("/usr/local/rocketmq-filemgr/conf")
	myfile.Statdir_and_mkdir("/usr/local/rocketmq-filemgr/core")
	myfile.Statdir_and_mkdir("/usr/local/rocketmq-filemgr/logs")
}

func coreInit() {
	cErr := syscall.Chdir("/usr/local/rocketmq-filemgr/core")
	if cErr != nil {
		log.Info(cErr)
		log.Flush()
		os.Exit(-1)
	}
	var rLimit syscall.Rlimit
	rLimit.Max = 60000
	rLimit.Cur = 60000

	var cLimit syscall.Rlimit
	cLimit.Max = 10000000000
	cLimit.Cur = 10000000000
	ccErr := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)
	if ccErr != nil {
		log.Info(ccErr)
		log.Flush()
		os.Exit(-1)
	}
	ccErr = syscall.Setrlimit(syscall.RLIMIT_CORE, &cLimit)
	if ccErr != nil {
		log.Info(ccErr)
		log.Flush()
		os.Exit(1)
	}
	os.Setenv("GOTRACEBACK", "crash")
}

func ConfigInit(cfgfilename string) error {
	go Watch(cfgfilename)
	return Parse(cfgfilename)
}

func ServersInit(serversFilename string) int {
	return 0
}



func init() {
	cfgInit()
	dirInit()
	cfgErr := ConfigInit("xxx")
	if cfgErr != nil {
		log.Info("ERROR: configInit", cfgErr)
		log.Flush()
		os.Exit(-1)
	}
	coreInit()
	transInit()
}
