package persistent_hashmap

import (
	"fmt"
	"os"
	"sync"
	"testing"
)

func task(p *PersistentHashmap, key string, value string, wg *sync.WaitGroup) {
	defer wg.Done()
	p.Put(key, value)
}

func TestPersistentHashmap_Put(t *testing.T) {
	type fields struct {
		AuditLogFilePath string
		AuditFileObject  *os.File
		Namespace        string
		HashMap          map[string]string
		mu               sync.Mutex
	}
	type args struct {
		key   string
		value string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "",
			fields: fields{
				AuditLogFilePath: "/Users/puneeth/Documents/software/side-projects/gfs-clone/mainserver_meta/persistent_hashmap_1_audit.log",
				Namespace:        "persistent_hashmap_1",
			},
			args: args{
				key:   "hello",
				value: "world",
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := PersistentHashmap{
				AuditLogFilePath: tt.fields.AuditLogFilePath,
				Namespace:        tt.fields.Namespace,
			}
			p.Initialize()
			var wg sync.WaitGroup
			// Use goroutines and run Put and Get parallely
			for i := 0; i < 10000; i++ {
				wg.Add(1)
				go task(&p, fmt.Sprintf("hello%d", i), fmt.Sprintf("world%d", i), &wg)
			}
			wg.Wait()
			p.AuditFileObject.Close()
		})
	}
}
