package batch

import (
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"log"
	"os"
	"reflect"
)

var (
	// fnMap contains the set of registered functions. See the `Register`
	// function.
	fnMap = map[string]interface{}{}
)

// Register stores information about functions so they can be called
// remotely. The function must take a pointer to a struct as an argument and
// return a single error.
func Register(fns ...interface{}) {
	for _, fn := range fns {
		fnType := reflect.TypeOf(fn)

		if fnType.NumIn() != 1 {
			panic("Expected function to take one argument.")
		}

		argType := fnType.In(0)
		if argType.Kind() != reflect.Ptr {
			panic("Expected function to take a pointer argument.")
		}

		if argType.Elem().Kind() != reflect.Struct {
			panic("Expected function to take a pointer to a struct.")
		}

		fnMap[functionName(fn)] = fn
	}
}

// Init must be called at the top of your program's main function after
// `Register`.
func Init() {
	if len(os.Args) < 4 || os.Args[1] != "__BATCH__" {
		return
	}

	// Get function name from command line.
	fnName := os.Args[2]
	encArgs, err := hex.DecodeString(os.Args[3])
	if err != nil {
		log.Fatalf("Failed to decode args: %v", err)
	}
	argsBuf := bytes.NewBuffer(encArgs)

	// Check that function exists.
	fnIface, ok := fnMap[fnName]
	if !ok {
		log.Fatalf("Unknown function: %s", fnName)
	}

	fnVal := reflect.ValueOf(fnIface)
	fnType := fnVal.Type()

	// Read the function arguments.
	argPVal := reflect.New(fnType.In(0))

	err = gob.NewDecoder(argsBuf).Decode(argPVal.Interface())
	if err != nil {
		log.Fatalf("Failed to decode task args: %v", err)
	}

	fnVal.Call([]reflect.Value{reflect.Indirect(argPVal)})

	// Success.
	os.Exit(0)
}
