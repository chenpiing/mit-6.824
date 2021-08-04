package raft

import "log"

// Debugging
const Debug = 0
const Debug1 = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func LockPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug1 > 0 {
		log.Printf(format, a...)
	}
	return
}
