package util

import (
	"runtime/debug"
	"strings"
)

func GetStackTrace() []string {
	rawStack := debug.Stack()
	if rawStack == nil {
		return []string{
			"{nil-stack-trace-returned-from-debug.Stack()}",
		}
	}

	stackStr := string(rawStack)
	split := strings.Split(stackStr, "\n")
	for i, s := range split {
		split[i] = strings.Replace(s, "\t", "    ", 1)
	}
	return split
}
