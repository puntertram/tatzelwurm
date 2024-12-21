package persistent_hashmap

import (
	"bufio"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
)

type PersistentHashmap struct {
	AuditLogFilePath string
	AuditFileObject  *os.File
	Namespace        string
	HashMap          map[string]string
	mu               sync.Mutex
}

func (p *PersistentHashmap) Initialize() bool {
	fmt.Println("Initializing object ", reflect.TypeOf(p))
	p.HashMap = make(map[string]string)
	_, err := os.Stat(p.AuditLogFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Printf("The audit log file path at %s does not exist!", p.AuditLogFilePath)
		}
		return false
	} else {
		p.AuditFileObject, err = os.OpenFile(p.AuditLogFilePath, os.O_RDWR, os.ModeAppend)
		// The err has to be nil as it would have been captured
		scanner := bufio.NewScanner(p.AuditFileObject)
		for scanner.Scan() {
			fmt.Println(scanner.Text())
			splitComponents := strings.Split(scanner.Text(), "|")
			request_type := splitComponents[0]
			if request_type == "add" {
				key := splitComponents[1]
				value := splitComponents[2]
				p.HashMap[key] = value
			} else if request_type == "remove" {
				key := splitComponents[1]
				delete(p.HashMap, key)
			} else if request_type == "update" {
				key := splitComponents[1]
				value := splitComponents[2]
				p.HashMap[key] = value
			}
		}
		return true
	}
}

func (p *PersistentHashmap) Get(key string) (string, bool) {
	value, ok := p.HashMap[key]
	return value, ok
}

func (p *PersistentHashmap) Put(key string, value string) bool {
	p.mu.Lock()
	result := false
	p.HashMap[key] = value
	if p.AuditFileObject == nil {
		result = false
	} else {
		p.AuditFileObject.Write([]byte(fmt.Sprintf("add|%s|%s\n", key, value)))
		p.AuditFileObject.Sync()
	}
	p.mu.Unlock()
	return result
}
func (p *PersistentHashmap) Check_value(value string) (string, bool) {
	// Do a reverse mapping, find a tuple (key, value) if it exists in the hashmap
	for key, _value := range p.HashMap {
		if _value == value {
			return key, true
		}
	}
	return "", false

}
