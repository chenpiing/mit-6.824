package rafttesg

import (
	"raft"
	"fmt"
	"time"
	"testing"
)

func TestTimer(t *testing.T) {
	rf := &raft.Raft{}
	fmt.Println(time.Now().UnixNano() / 1e6)
	timeChan := make(chan int64)
	wakeupChan := make(chan int)
	resChan := make(chan bool)

	go func() {
		rf.StartTimer(timeChan, wakeupChan, resChan)
	} ()

	res := rf.StartSleep(1000, timeChan, resChan)
	fmt.Println(res)
	fmt.Println(time.Now().UnixNano() / 1e6)

	go func() {
		time.Sleep(time.Duration(200) * time.Millisecond)
		rf.StopSleep(wakeupChan)
	} ()

	res = rf.StartSleep(1000, timeChan, resChan)
	fmt.Println(res)
	fmt.Println(time.Now().UnixNano() / 1e6)
}

func TestSlice(t *testing.T) {
	arr := make([]int, 10)
	for i := 0; i < 10; i++ {
		arr[i] = i
	}
	for i := range arr {
		fmt.Println(i)
	}
	slice := arr[5:]
	for _, v := range slice {
		fmt.Println(v)
	}
}