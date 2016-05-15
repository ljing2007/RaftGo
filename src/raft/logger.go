package raft

import (
	"log"
	"io"
	"io/ioutil"
)

var (
	Trace   *log.Logger
	Info    *log.Logger
	Warning *log.Logger
	Error   *log.Logger
)

// init logger
func InitLogger(traceHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer) {
	Trace = log.New(traceHandle,
		"TRACE: ", log.Ldate|log.Ltime|log.Lshortfile)

	Info = log.New(infoHandle,
		"INFO: ", log.Ldate|log.Ltime|log.Lshortfile)

	Warning = log.New(warningHandle,
		"WARNING: ", log.Ldate|log.Ltime|log.Lshortfile)

	Error = log.New(errorHandle,
		"ERROR: ", log.Ldate|log.Ltime|log.Lshortfile)
}


func DisableLogger() {
	Trace.SetOutput(ioutil.Discard)
	Info.SetOutput(ioutil.Discard)
	Warning.SetOutput(ioutil.Discard)
	Error.SetOutput(ioutil.Discard)
}

func EnableLogger(traceHandle io.Writer, infoHandle io.Writer, warningHandle io.Writer, errorHandle io.Writer) {
	Trace.SetOutput(traceHandle)
	Info.SetOutput(infoHandle)
	Warning.SetOutput(warningHandle)
	Error.SetOutput(errorHandle)
}
