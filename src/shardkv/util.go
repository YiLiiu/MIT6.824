package shardkv

import "log"

// Debugging
const Debug = 0

func DPrintf(args ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Println(args...)
	}
	return
}
