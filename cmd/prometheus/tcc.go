package main

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"code.byted.org/gopkg/tccclient"
)

type TccHandler struct {
	configChanged chan bool
}

func NewTccHandler(psm, key, filePath string) *TccHandler {
	c := &TccHandler{configChanged: make(chan bool)}
	client, err := tccclient.NewClientV2(psm, tccclient.NewConfigV2())
	if err != nil {
		log.Println("new tcc client failed", err)
		return c
	}
	go wait.Forever(func() {
		data, err := ioutil.ReadFile(filePath)
		if err != nil {
			log.Println("read config file error", err)
			return
		}
		tccData, err := client.Get(context.Background(), key)
		if err != nil {
			log.Println("get tcc config error", err)
			return
		}
		tccDataByte := []byte(tccData)
		if bytes.EqualFold(data, tccDataByte) {
			return
		}

		newFile, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
		if err != nil {
			log.Println("open config file error", err)
			return
		}
		defer func() { _ = newFile.Close() }()
		_, err = newFile.Write(tccDataByte)
		if err != nil {
			log.Println("write config file error", err)
			return
		}
		c.configChanged <- true
	}, time.Second*15)
	return c
}

func (c *TccHandler) waitTccLoad(timeout time.Duration) {
	select {
	case <-time.After(timeout):
		log.Println("waitTccLoad timeout")
		return
	case <-c.configChanged:
		log.Println("waitTccLoad success")
		return
	}
}
