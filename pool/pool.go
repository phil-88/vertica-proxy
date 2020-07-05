/*
Copyright 2017 Crunchy Data Solutions, Inc.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package pool

import (
	"net"
	"time"
)

type Connection struct {
	Name          string
	Role          string
	Con           net.Conn
	IsOk          bool
	Ts            time.Time
	Expired       bool
	Detached      bool
	BackendPid    int32
	BackendSecret int32
	Autocommit    bool
	Owner         string
}

type Client struct {
	CleintPid     int32
	CleintSecret  int32
	BackendPid    int32
	BackendSecret int32
	BackendName   string
	BackendRole   string
	IsRunning     bool
	StatementNo   int
}

type Pool struct {
	connections chan Connection
	Name        string
	Private     bool
}

func NewPool(name string, capacity int) *Pool {
	return &Pool{
		connections: make(chan Connection, capacity),
		Name:        name,
		Private:     false,
	}
}

func NewDedicatedPool(role string, backend net.Conn, pid int32, secret int32, userName string) *Pool {
	dedicatedPool := &Pool{
		connections: make(chan Connection, 1),
		Name:        "dedicated " + role,
		Private:     true,
	}

	c := Connection{
		Name:          backend.RemoteAddr().String(),
		Role:          role,
		Con:           backend,
		IsOk:          true,
		Ts:            time.Now(),
		BackendPid:    pid,
		BackendSecret: secret,
		Autocommit:    false,
		Owner:         userName,
	}
	dedicatedPool.Add(c)

	return dedicatedPool
}

func (p *Pool) Add(connection Connection) {
	p.connections <- connection
}

func (p *Pool) Pop() Connection {
	return <-p.connections
}

func (p *Pool) PopNoBlock() Connection {
	select {
	case msg := <-p.connections:
		return msg
	default:
		return Connection{}
	}
}

func (p *Pool) Push(connection Connection) {
	p.connections <- connection
}

func (p *Pool) Len() int {
	return len(p.connections)
}
