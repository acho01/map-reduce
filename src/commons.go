package main

import (
	"strconv"
	"strings"
	"unicode"
)

// Task Type
const (
	MAP    = 0
	REDUCE = 1
)

// Worker Status
const (
	IDLE        = 0
	IN_PROGRESS = 1
	COMPLETED   = 2
)

type KeyValue struct {
	Key   string
	Value string
}

func Map(filename string, contents string) []KeyValue {
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

func Reduce(key string, values []string) string {
	return strconv.Itoa(len(values))
}
