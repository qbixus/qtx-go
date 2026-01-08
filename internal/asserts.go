package internal

import (
	"fmt"
	"log"
	"runtime"
)

func Assert(condition bool, tags ...any) {
	if !condition {
		tags = append([]any{"#ASSERTION_FAILED"}, tags...)
		if _, file, line, ok := runtime.Caller(1); ok {
			tags = append(tags, "\n\t"+fmt.Sprintf("%v:%v", file, line))
		}
		if _, file, line, ok := runtime.Caller(2); ok {
			tags = append(tags, "\n\t"+fmt.Sprintf("%v:%v", file, line))
		}
		log.Fatal(tags...)
	}
}

func AssertFunc(condition func() bool, tags ...any) {
	if !condition() {
		tags = append([]any{"#ASSERTION_FAILED"}, tags...)
		if _, file, line, ok := runtime.Caller(1); ok {
			tags = append(tags, "\n\t"+fmt.Sprintf("%v:%v", file, line))
		}
		if _, file, line, ok := runtime.Caller(2); ok {
			tags = append(tags, "\n\t"+fmt.Sprintf("%v:%v", file, line))
		}
		log.Fatal(tags...)
	}
}
