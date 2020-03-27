package batch

import (
	"reflect"
	"runtime"
)

func functionName(i interface{}) string {
	return runtime.FuncForPC(reflect.ValueOf(i).Pointer()).Name()
}
