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

package proxy

import (
	"fmt"
	"io"
	"net"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/crunchydata/crunchy-proxy/config"
	"github.com/crunchydata/crunchy-proxy/common"
	"github.com/crunchydata/crunchy-proxy/connect"
	"github.com/crunchydata/crunchy-proxy/pool"
	"github.com/crunchydata/crunchy-proxy/protocol"
	"github.com/crunchydata/crunchy-proxy/util/log"
)

/*

1. simple query:

> Q 0 0 0 14 select 3; 0
< T C Z

> Q 0 0 0 14 select '' + 3; 0
< E Z


2. extended query:

jdbc:

named prepare+flush:
> P 0 0 0 Ps1 0 SET search_path = public,"$user",public,v_catalog,v_monitor,v_internal 0 0 0 D 0 0 0 8 Ss1 0 H 0 0 0 4
< 1 t n m
> B 0 0 0 14 0 s1 0 0 0 0 0 0 0 E 0 0 0 9 0 0 0 0 0 S 0 0 0 4
< 2 C Z
> C 0 0 0 8 Ss1 0 H 0 0 0 4
< 3

anon prepare+flush:
> P 0 0 0 16 0 select 5 0 0 0 H 0 0 0 4
< m 1
> Q 0 0 0 13 select 5 0
< T C Z

odbc:

named prepare+sync:
> P 0 0 0 O_PLAN000000CF6036B3C0_0 0 SELECT SESSION_ID FROM V_MONITOR.CURRENT_SESSION 0 0 0 S 0 0 0 4
< 1 Z
> D 0 0 0 29 S_PLAN000000CF6036B3C0_0 0 S 0 0 0 4
< t T m Z
> B 0 0 0 % 0 _PLAN000000CF6036B3C0_0 0 0 0 0 0 0 1 0 0 D 0 0 0 6 P 0 E 0 0 0 9 0 0 0 0 0 S 0 0 0 4
< 2 T s Z
> C 0 0 0 29 S_PLAN000000CF6036B3C0_0 0 + next query + sync
< 3 + next resp

3. copy

vsql:
> Q%copy tt from local '/tmp/t.csv';
< T F
> F/tmp/t.csv
< S S S H
> d j
< J c C Z

tableau:
> P 0 0 1 4 _PLAN000000D18637BA40_4 0 COPY "_tableau_2_v_dwh_node0014_2709_0xea78c_1_connect" FROM LOCAL 'C:\Users\8523~1\AppData\Local\Temp\TableauTemp\0ls9plh0j8qrdj1djyagd1c1fuqz\table000.tbl' DELIMITER '|' NULL E'\\N' DIRECT RECORD TERMINATOR E'\n' ABORT ON ERROR 0 0 0 S 0 0 0 4
< 1 Z
> D 0 0 0 29 S_PLAN000000D18637BA40_4 0 S 0 0 0 4
< t n m Z
> P 0 0 0 237 0 COPY "_tableau_2_v_dwh_node0014_2709_0xea78c_1_connect" FROM LOCAL 'C:\Users\8523~1\AppData\Local\Temp\TableauTemp\0ls9plh0j8qrdj1djyagd1c1fuqz\table000.tbl' DELIMITER '|' NULL E'\\N' DIRECT RECORD TERMINATOR E'\n' ABORT ON ERROR 0 0 0 S 0 0 0 4
< m 1 Z
> Q 0 0 0 234 COPY "_tableau_2_v_dwh_node0014_2709_0xea78c_1_connect" FROM LOCAL 'C:\Users\8523~1\AppData\Local\Temp\TableauTemp\0ls9plh0j8qrdj1djyagd1c1fuqz\table000.tbl' DELIMITER '|' NULL E'\\N' DIRECT RECORD TERMINATOR E'\n' ABORT ON ERROR 0
< T F
> F 0 0 0 g 0 1 C:\Users\8523~1\AppData\Local\Temp\TableauTemp\0ls9plh0j8qrdj1djyagd1c1fuqz\table000.tbl 0 0 0 0 0 0 0 0 2
< S S S H
> d 0 0 0 6 1 10 j 0 0 0 4
< J c C Z
> C 0 0 0 29 S_PLAN000000D18637BA40_4 0 + next query + sync
< 3 + next resp

> Q&copy tt from local '/tmp/t2.csv';
< T F
> e
< E Z

> P 0 0 0 5 0 copy tt from local '/tmp/t.csv' delimiter ',' 0 0 0 H 0 0 0 4
< m 1
> Q 0 0 0 2copy tt from local '/tmp/t.csv' delimiter ',' 0
< T F
> F 0 0 0 25 0 1 /tmp/t.csv 0 0 0 0 0 0 0 0 27
< S S S H
> d 0 0 0 31 1,'a' 10 2,'bb' 10 3,'ccc' 10 4,'d' 10 j 0 0 0 4
< J c C Z
> c S
< Z

*/

type Proxy struct {
	pools   map[string](chan *pool.Pool)
	clients map[string]pool.Client
	Stats   map[string]int32
	lock    *sync.Mutex
}

func NewProxy() *Proxy {
	p := &Proxy{
		Stats:   make(map[string]int32),
		lock:    &sync.Mutex{},
		clients: make(map[string]pool.Client),
		pools:   make(map[string](chan *pool.Pool)),
	}

	p.setupPools()

	reconnectTicker := time.NewTicker(time.Millisecond * 5000)
	go func() {
		for _ = range reconnectTicker.C {
			p.reconnect()
		}
	}()

	expireTicker := time.NewTicker(time.Millisecond * 60000)
	go func() {
		for _ = range expireTicker.C {
			p.removeExpiredConnections()
		}
	}()

	return p
}

func (p *Proxy) setupPools() {
	nodes := config.GetNodes()
	capacity := config.GetPoolCapacity()
	capacityDedicated := 10000
	numNodes := len(nodes)

	for nodeName, node := range nodes {

		if !config.GetPoolEnable() && node.Role == config.GetDefaultBackend() {
			continue
		}

		_, ok := p.pools[node.Role]
		if !ok {
			poolChan := make(chan *pool.Pool, 1)
			poolChan <- pool.NewPool("shared "+node.Role, capacity*numNodes)
			p.pools[node.Role] = poolChan
		}

		var rolePool *pool.Pool
		rolePool = <-p.pools[node.Role]

		/* Create connections and add to pool. */
		for i := 0; i < capacity; i++ {
			log.Infof("Connecting to node '%s' at %s...", nodeName, node.HostPort)

			connection, err := connect.Connect(node.HostPort, node.Role)
			if err != nil {
				log.Errorf("Error establishing connection to node '%s'", nodeName)
				log.Errorf("Error: %s", err.Error())
				continue
			}

			log.Infof("Connection to '%s' at %s established...", nodeName, node.HostPort)

			isAuthenticated, backendPid, backendSecret := connect.AuthenticateBackend(connection, node.Role)
			if isAuthenticated {
				log.Infof("Successfully authenticated with '%s' at '%s' (pid %d, secret %d)", nodeName, node.HostPort, backendPid, backendSecret)
				c := pool.Connection{
					Name:          nodeName,
					Role:          node.Role,
					Con:           connection,
					IsOk:          true,
					Ts:            time.Now(),
					BackendPid:    backendPid,
					BackendSecret: backendSecret,
					Autocommit:    false,
					Owner:         "",
				}
				rolePool.Add(c)
			} else {
				log.Error("Authentication failed")
			}
		}

		p.pools[node.Role] <- rolePool
	}

	poolChan := make(chan *pool.Pool, 1)
	poolChan <- pool.NewPool("broken", capacity*numNodes)
	p.pools["broken"] = poolChan

	dedicatedPoolChan := make(chan *pool.Pool, capacityDedicated*numNodes)
	p.pools["dedicated"] = dedicatedPoolChan
}

func (p *Proxy) reconnect() {

	brokenPool := <-p.pools["broken"]
	brokenCount := brokenPool.Len()

	for brokenCount > 0 {
		brokenCount = brokenCount - 1
		connection := brokenPool.Pop()

		if !connection.IsOk {
			address := connection.Con.RemoteAddr().String()
			log.Infof("Reconnecting to node '%s' at %s...", connection.Name, address)

			con, err := connect.Connect(address, connection.Role)
			if err != nil {
				log.Errorf("pool check: Error establishing connection to '%s'", connection.Name)
				log.Errorf("pool check: Error: %s", err.Error())
				continue
			}

			isAuthenticated, backendPid, backendSecret := connect.AuthenticateBackend(con, connection.Role)
			if isAuthenticated {
				log.Infof("Successfully reconnected to '%s' at '%s' (pid %d, secret %d)", connection.Name, address, backendPid, backendSecret)
				connection.Con = con
				connection.IsOk = true
				connection.Ts = time.Now()
				connection.BackendPid = backendPid
				connection.BackendSecret = backendSecret
				connection.Autocommit = false

				rolePool := <-p.pools[connection.Role]
				rolePool.Add(connection)
				p.pools[connection.Role] <- rolePool

			} else {
				log.Error("pool check: Authentication failed")
				brokenPool.Add(connection)
			}
		}
	}

	p.pools["broken"] <- brokenPool
}

// travers pool and remove connects with expired time to live
func (p *Proxy) removeExpiredConnections() {
	poolCount := len(p.pools["dedicated"])
	for i := 0; i < poolCount; i++ {
		pool := <-p.pools["dedicated"]
		if pool.Len() == 1 {
			con := pool.PopNoBlock()
			if con.Expired {
				pool.Push(con)
				continue
			}
			if con.Con == nil {
				continue
			}
			durationInSeconds := int(time.Since(con.Ts).Seconds())
			if durationInSeconds > config.GetConnectionTTL() {
				con.Con.Close()
				pool.Push(con)
				continue
			}
			pool.Push(con)
		}
		p.pools["dedicated"] <- pool
	}
}

// Switch the current connection to the connection from pool[dedicated] with the input sessionID
func (p *Proxy) pickSession(userName string, sessionID string, dedicatedPool *pool.Pool) bool {
	switched := false

	poolCount := len(p.pools["dedicated"])
	for i := 0; i < poolCount && !switched; i++ {
		pool := <-p.pools["dedicated"]
		if pool.Len() == 1 {
			con := pool.Pop()
			if (config.GetPickSessionLevel() == "any" || config.GetPickSessionLevel() == "self" && userName == con.Owner) &&
				fmt.Sprint(con.BackendPid) == sessionID && dedicatedPool != pool {

				switched = true
				con.Ts = time.Now()

				localCon := dedicatedPool.Pop()
				if con.Detached {
					localCon.Con.Close()
					localCon.Expired = true
				}
				pool.Push(localCon)
				dedicatedPool.Push(con)
			} else {
				pool.Push(con)
			}
		}
		p.pools["dedicated"] <- pool
	}
	return switched
}

// Get the next pool. If read is set to true, then a 'read-only' pool will be
// returned. Otherwise, a 'read-write' pool will be returned.
func (p *Proxy) popPool(querySpec map[AnnotationType]string, dedicatedPool *pool.Pool) *pool.Pool {

	poolChan, ok := p.pools[querySpec[RouteAnnotation]]
	if ok {
		return <-poolChan
	} else if dedicatedPool != nil {
		dedicatedPool.Private = true
		return dedicatedPool
	} else {
		return <-p.pools[config.GetDefaultBackend()]
	}
}

// Return the pool. If read is 'true' then, the pool will be returned to the
// 'read-only' collection of pools. Otherwise, it will be returned to the
// 'read-write' collection of pools.
func (p *Proxy) pushPool(pl *pool.Pool, querySpec map[AnnotationType]string) {
	if pl.Private {
		return
	}
	poolChan, ok := p.pools[querySpec[RouteAnnotation]]
	if ok {
		poolChan <- pl
	} else {
		p.pools[config.GetDefaultBackend()] <- pl
	}
}

func (p *Proxy) releaseConnection(currentPool *pool.Pool, backendRecord pool.Connection, clientName string) {
	backend := backendRecord.Con
	if currentPool != nil && backend != nil {

		p.lock.Lock()
		_, ok := p.clients[clientName]
		if ok {
			delete(p.clients, clientName)
		}
		p.lock.Unlock()

		if backendRecord.IsOk || currentPool.Private {
			currentPool.Push(backendRecord)
		} else {
			brokenPool := <-p.pools["broken"]
			brokenPool.Add(backendRecord)
			p.pools["broken"] <- brokenPool
		}
		log.Debugf("%s: Backend %s unlocked (%d %d), %d connections left in pool %s", clientName,
			backendRecord.Name, backendRecord.BackendPid, backendRecord.BackendSecret, currentPool.Len(), currentPool.Name)
	}
}

func (p *Proxy) getMasterNode() common.Node {
	nodes := config.GetNodes()
	nodeName := config.GetDefaultBackend()
	for name, node := range nodes {
		if node.Role == config.GetDefaultBackend() {
			nodeName = name
		}
	}
	return nodes[nodeName]
}

func (p *Proxy) getMasterConnection() (net.Conn, error) {
	node := p.getMasterNode();

	/* Establish a connection with the master node. */
	master, err := connect.Connect(node.HostPort, config.GetDefaultBackend())
	if err != nil {
		log.Error("An error occurred connecting to the master node")
		log.Errorf("Error %s", err.Error())
		return nil, err
	}
	return master, err
}

// HandleConnection handle an incoming connection to the proxy
func (p *Proxy) HandleConnection(client net.Conn) {

	clientName := client.RemoteAddr().String()

	/* Get the client startup message. */
	message, length, err := connect.RecieveStartupMessage(client)

	log.Debugf("%s: Startup message: %s", clientName, log.GetReadableDatagram(message[:length], length))

	if err != nil || length < 4 {
		log.Errorf("%s: Error receiving startup message from client.", clientName)
		log.Errorf("%s: Error: %s", clientName, err.Error())
		return
	}

	/* Get the protocol from the startup message.*/
	startupProtocol := protocol.GetStartupProtocol(message)

	/* Handle the case where the startup message was an SSL request. */
	if startupProtocol == protocol.SSLRequestCode {
		sslResponse := protocol.NewMessageBuffer([]byte{})

		/* Determine which SSL response to send to client. */
		creds := config.GetCredentials(config.GetDefaultBackend())
		if creds.SSL.Enable {
			sslResponse.WriteByte(protocol.SSLAllowed)
		} else {
			sslResponse.WriteByte(protocol.SSLNotAllowed)
		}

		/*
		 * Send the SSL response back to the client and wait for it to send the
		 * regular startup packet.
		 */
		connect.Send(client, sslResponse.Bytes())

		/* Upgrade the client connection if required. */
		client = connect.UpgradeServerConnection(client, config.GetDefaultBackend())

		/*
		 * Re-read the startup message from the client. It is possible that the
		 * client might not like the response given and as a result it might
		 * close the connection. This is not an 'error' condition as this is an
		 * expected behavior from a client.
		 */
		if message, length, err = connect.RecieveStartupMessage(client); err == io.EOF {
			log.Infof("%s: The client closed the connection.", clientName)
			return
		}

		log.Debugf("%s: Startup message after ssl: %s", clientName, log.GetReadableDatagram(message[:length], length))
	}

	/* check for CancelRequest */
	if startupProtocol == protocol.CancelRequest {
		clientPid := protocol.GetBackendPid(message)
		clientSecret := protocol.GetBackendSecret(message)
		statementNo := -1
		log.Infof("%s: cancel request (pid %d, secret %d)", clientName, clientPid, clientSecret)

		for i := 0; i < 3; i++ {
			var cancelRequest []byte
			var backendRole string

			p.lock.Lock()
			for _, clientRecord := range p.clients {
				if clientRecord.IsRunning && (statementNo == -1 || statementNo == clientRecord.StatementNo) &&
					clientRecord.CleintPid == clientPid && clientRecord.CleintSecret == clientSecret {

					cancelRequest = protocol.GetCancelRequest(clientRecord.BackendPid, clientRecord.BackendSecret)
					backendRole = clientRecord.BackendRole
					statementNo = clientRecord.StatementNo
					break
				}
			}
			p.lock.Unlock()

			if len(cancelRequest) > 0 {
				nodes := config.GetNodes()

				nodeName := config.GetDefaultBackend()
				for name, node := range nodes {
					if node.Role == backendRole {
						nodeName = name
					}
				}
				node := nodes[nodeName]

				master, err := connect.Connect(node.HostPort, backendRole)
				if err != nil {
					log.Errorf("%s: An error occurred connecting to node %s during CancelRequest", clientName, nodeName)
					log.Errorf("%s: Error %s", clientName, err.Error())
					return
				}

				connect.Send(master, cancelRequest)

				response, length, _ := connect.Receive(master, 4096)
				connect.Send(client, response[:length])
				log.Infof("%s: cancel response from %s: %s", clientName, nodeName, response[:length])

				master.Close()
			} else {
				log.Infof("%s: query canceled (pid %d, secret %d)", clientName, clientPid, clientSecret)
				return
			}
			time.Sleep(time.Second * 5)
		}

		log.Infof("%s: cancel request timedout (pid %d, secret %d)", clientName, clientPid, clientSecret)
		return
	}

	/*
	 * Validate that the client username and database are the same as that
	 * which is configured for the proxy connections.
	 */
	creds := config.GetCredentials(config.GetDefaultBackend())
	userName, dbName, clientType, clientVersion := protocol.GetStartupClientCredits(message)
	clientName = clientName + "(" + userName + "@" + dbName + ")"
	clientIsODBC, _ := regexp.MatchString("(?i)ODBC", clientType)
	clientIsJDBC, _ := regexp.MatchString("(?i)JDBC", clientType)

	if strings.ToLower(dbName) != strings.ToLower(creds.Database) || !config.HasClient(userName, -1) {

		pgError := protocol.Error{
			Severity: protocol.ErrorSeverityError,
			Code:     protocol.ErrorCodeInvalidAuthorizationSpecification,
			Message:  "User or database name is invalid",
		}
		connect.Send(client, pgError.GetMessage())

		readyMsg := protocol.GetTransactionOkMessage(false)
		connect.Send(client, readyMsg)

		log.Errorf("%s: Cannot validate client. Valid users: %s", clientName, config.GetConfig().Clients)
		return
	}

	clientVersionInt := protocol.ParseClientVersionString(clientVersion)
	if clientVersionInt != 0 && clientVersionInt < 0x080000 && (clientIsODBC || clientIsJDBC) {

		pgError := protocol.Error{
			Severity: protocol.ErrorSeverityError,
			Code:     protocol.ErrorCodeInvalidAuthorizationSpecification,
			Message:  "Client version not supported " + clientVersion,
		}
		connect.Send(client, pgError.GetMessage())

		readyMsg := protocol.GetTransactionOkMessage(false)
		connect.Send(client, readyMsg)

		log.Errorf("%s: Client version not supported %s", clientName, clientVersion)
		return
	}

	/* Authenticate the client against the appropriate backend. */
	log.Infof("%s: client authenticating", clientName)

	clientProtocolVersion := protocol.ParseProtocolVersionString(config.GetCredentials(config.GetDefaultBackend()).Protocol)

	var dedicatedPool *pool.Pool = nil
	var dedicatedBackend net.Conn
	var dedicatedDetached bool
	var authenticated bool
	var authMethod int32
	var pid int32
	var secret int32

	if config.GetPoolEnable() {

		authenticated, err, pid, secret = connect.FakeAuthenticateClient(client, message, length, clientName)

		if !authenticated {
			log.Errorf("%s: client authentication failed", clientName)
			return
		} else {
			log.Debugf("%s: client authentication successful (pid %d, secret %d)", clientName, pid, secret)
		}

	} else {
		var err error

		log.Debugf("%s: client auth: connecting to 'master' node", clientName)
		dedicatedBackend, err = p.getMasterConnection()
		if dedicatedBackend == nil || err != nil {
			log.Errorf("%s: client authentication failed", clientName)
			return
		}

		authenticated, err, authMethod, pid, secret = connect.AuthenticateClient(client, dedicatedBackend, message, length, clientName, false)

		if !authenticated {
			log.Errorf("%s: client authentication failed", clientName)
			dedicatedBackend.Close()
			return
		} else if !config.HasClient(userName, authMethod) {
			pgError := protocol.Error{
				Severity: protocol.ErrorSeverityError,
				Code:     protocol.ErrorCodeInvalidAuthorizationSpecification,
				Message:  "User auth method not supported",
			}
			connect.Send(client, pgError.GetMessage())

			readyMsg := protocol.GetTransactionOkMessage(false)
			connect.Send(client, readyMsg)

			log.Errorf("%s: client authentication method not supported: %d", clientName, authMethod)
			dedicatedBackend.Close()
			return
		} else {
			log.Debugf("%s: client authentication successful (pid %d, secret %d)", clientName, pid, secret)

			readyMsg := protocol.GetTransactionOkMessage(false)
			connect.Send(client, readyMsg)
		}

		dedicatedPool = pool.NewDedicatedPool(config.GetDefaultBackend(), dedicatedBackend, pid, secret, userName)
		p.pools["dedicated"] <- dedicatedPool
		log.Debugf("%s: lenght of dedicated chan is %d, cap is %d", clientName, len(p.pools["dedicated"]), cap(p.pools["dedicated"]))

		defer func() {

			backendRecord := pool.Connection{Expired: true}
			if dedicatedPool.Len() > 0 {
				backendRecord = dedicatedPool.Pop()
				dedicatedBackend = backendRecord.Con
				dedicatedDetached = backendRecord.Detached
			}

			if !dedicatedDetached || backendRecord.Expired {
				log.Debugf("%s: backend closed", clientName)
				dedicatedBackend.Close()
				dedicatedPool.Push(pool.Connection{Expired: true})
			} else {
				log.Debugf("%s: backend detached", clientName)
				dedicatedPool.Push(backendRecord)
			}

		}()
	}

	clientRecord := pool.Client{
		CleintPid:     pid,
		CleintSecret:  secret,
		BackendPid:    int32(0),
		BackendSecret: int32(0),
		BackendRole:   "",
		BackendName:   "",
		IsRunning:     false,
		StatementNo:   0,
	}

	/* Process the client messages for the life of the connection. */
	var holdConnection bool
	var commandBlock bool
	var autocommit bool
	var namedPrepare string
	var pendingMessage []byte
	var pendingLength int
	var pendingResponse bool = false

	var currentPool *pool.Pool // The connection pool in use
	var backendRecord pool.Connection
	var backend net.Conn // The backend connection in use
	var poolName string

	responseTimeout := 30 * time.Second
	forceRouting := config.GetForceRouting() && clientIsODBC

	log.Infof("%s: Start client event loop", clientName)

	for {
		var done bool // for message processing loop.

		if pendingLength == 0 {
			message, length, err = connect.RecievePendingMessages(client)
		} else {
			message = pendingMessage
			length = pendingLength
			err = nil
			pendingLength = 0
		}

		if err != nil {
			switch err {
			case io.EOF:
				log.Infof("%s: Client closed the connection", clientName)
			default:
				log.Errorf("%s: Error reading from client connection", clientName)
				log.Errorf("%s: Error: %s", clientName, err.Error())
			}
			p.releaseConnection(currentPool, backendRecord, clientName)
			return
		}

		requestType := protocol.GetMessageType(message)
		if requestType != protocol.CopyDataMessageType {
			log.Infof("%s: Request from client: %s", clientName, log.GetReadableDatagram(message[:length], length))
		}

		/*
		 * If the message is a simple query, then it can have read/write
		 * annotations attached to it. Therefore, we need to process it and
		 * determine which backend we need to send it to.
		 */
		if requestType == protocol.TerminateMessageType {
			log.Infof("%s: client disconnected", clientName)
			p.releaseConnection(currentPool, backendRecord, clientName)
			return
		} else if requestType == protocol.QueryMessageType ||
			requestType == protocol.ParseMessageType ||
			requestType == protocol.DescribeMessageType ||
			requestType == protocol.BindMessageType ||
			requestType == protocol.CloseMessageType ||
			requestType == protocol.SyncMessageType ||
			requestType == protocol.FlushMessageType ||
			requestType == protocol.CopyHeaderMessageType ||
			requestType == protocol.CopyErrorMessageType ||
			requestType == protocol.CopyDataMessageType ||
			requestType == protocol.CopyDataSentMessageType ||
			requestType == protocol.CopyDoneMessageType {

			/* parse input: check for flush or sync in extended query */
			var waitingForResponse bool = false
			var syncIssued bool = false
			var flushIssued bool = false
			var queryIssued bool = false
			var prepareIssued bool = false
			var anonPrepareIssued bool = false
			var describeIssued bool = false

			start := 0
			queryStart := 0
			prevTail := []byte{}
			newHead := []byte{}
			for start+5 <= length {
				msgType := protocol.GetMessageType(message[start:])
				msgLength := protocol.GetMessageLength(message[start:])

				if msgType == protocol.QueryMessageType {
					waitingForResponse = true
					queryIssued = true
					queryStart = start
				} else if msgType == protocol.ParseMessageType {
					prepareIssued = true
					queryStart = start
					name := protocol.ParseName(message[start:length], int(msgLength)+1)
					if name == "" {
						anonPrepareIssued = true
					}
					if namedPrepare == "" {
						namedPrepare = name
					}
				} else if msgType == protocol.DescribeMessageType {
					name := protocol.ParseName(message[start:length], int(msgLength)+1)
					if name != "" {
						describeIssued = true
					}
				} else if msgType == protocol.CloseMessageType {
					name := protocol.ParseName(message[start:length], int(msgLength)+1)
					if namedPrepare == name {
						namedPrepare = ""
					}
				} else if msgType == protocol.FlushMessageType {
					waitingForResponse = true
					flushIssued = true
				} else if msgType == protocol.SyncMessageType {
					waitingForResponse = true
					syncIssued = true
				} else if msgType == protocol.CopyErrorMessageType {
					waitingForResponse = true
				} else if msgType == protocol.CopyHeaderMessageType {
					waitingForResponse = true
				} else if msgType == protocol.CopyDataSentMessageType {
					waitingForResponse = true
				}

				if forceRouting {
					if queryIssued || prepareIssued {
						newHead = append(newHead, message[start:start+int(msgLength)+1]...)
					} else {
						prevTail = append(prevTail, message[start:start+int(msgLength)+1]...)
					}
				}

				start = (start + int(msgLength) + 1)
			}

			annotations := parseQuery(message[queryStart:])

			/* split pipelined queries to force pool switch */
			log.Debugf("%s: tx=%t q=%t p=%t n=%s s=%t offset=%d len=%d", clientName, commandBlock, queryIssued, prepareIssued, namedPrepare, syncIssued, queryStart, length)

			if forceRouting && commandBlock && pendingResponse && queryStart > 0 && (queryIssued || prepareIssued && syncIssued) &&
				annotations[RouteAnnotation] != "" && annotations[RouteAnnotation] != backendRecord.Role {

				pendingMessage = []byte{}
				pendingMessage = append(pendingMessage, newHead...)
				pendingLength = len(pendingMessage) //length

				message = []byte{}
				message = append(message, prevTail...)
				length = len(message)

				log.Infof("%s: client query splited on [%s] and [%s]. Routing sync first", clientName,
					log.GetReadableDatagram(message[:length], length),
					log.GetReadableDatagram(pendingMessage[:pendingLength], pendingLength))
			}

			/* force pool switch */
			if forceRouting && currentPool != nil && backend != nil && !commandBlock &&
				(requestType == protocol.QueryMessageType || requestType == protocol.ParseMessageType) &&
				annotations[RouteAnnotation] != "" && annotations[RouteAnnotation] != backendRecord.Role {

				p.releaseConnection(currentPool, backendRecord, clientName)
				currentPool = nil
				backend = nil
				backendRecord = pool.Connection{}
				holdConnection = false
			}

			/* handle session state */
			if requestType != protocol.QueryMessageType {
				commandBlock = true
			}

			if annotations[SessionLockAnnotation] != "" {
				log.Infof("%s: keep connection due to %s", clientName, annotations[SessionLockAnnotation])
				holdConnection = true
			} else if annotations[SessionUnlockAnnotation] != "" {
				holdConnection = false
				autocommit = false
			}

			if annotations[AutocommitAnnotation] == "on" {
				autocommit = true
			} else if annotations[AutocommitAnnotation] == "off" {
				autocommit = false
			}

			if annotations[DMLAnnotation] != "" && !autocommit {
				log.Infof("%s: keep connection due to %s", clientName, annotations[DMLAnnotation])
				holdConnection = true
			}

			/* fast process query */
			if annotations[NotSupportedAnnotation] != "" {
				log.Infof("%s: %s not supported", clientName, annotations[NotSupportedAnnotation])
				pgError := protocol.Error{
					Severity: protocol.ErrorSeverityError,
					Code:     protocol.ErrorCodeFeatureNotSupported,
					Message:  annotations[NotSupportedAnnotation] + " not supported",
				}
				connect.Send(client, pgError.GetMessage())

				if commandBlock {
					tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
					log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
					if err != nil {
						return
					}
					commandBlock = false
				}
				readyMsg := protocol.GetTransactionOkMessage(false)
				connect.Send(client, readyMsg)

				continue
			}

			if annotations[RouteAnnotation] == "abort" {
				log.Infof("%s: Query aborted", clientName)
				pgError := protocol.Error{
					Severity: protocol.ErrorSeverityError,
					Code:     protocol.ErrorCodeFeatureNotSupported,
					Message:  "Dangerous query detected. Aborted",
				}
				connect.Send(client, pgError.GetMessage())

				if commandBlock {
					tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
					log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
					if err != nil {
						return
					}
					commandBlock = false
				}
				readyMsg := protocol.GetTransactionOkMessage(false)
				connect.Send(client, readyMsg)

				continue
			}

			if annotations[AutocommitAnnotation] != "" && protocol.IsVertica(clientProtocolVersion) && config.GetPoolEnable() {
				log.Infof("%s: pend autocommit for future", clientName)

				if requestType == protocol.ParseMessageType {
					bindMsg := protocol.GetNeedSimpleRequestMessage()
					completeMsg := protocol.GetParseCompleteMessage()
					connect.Send(client, append(bindMsg, completeMsg...))

				} else if requestType == protocol.QueryMessageType {

					statusMsg := protocol.GetParameterStatusMessage("auto_commit", annotations[AutocommitAnnotation])
					connect.Send(client, statusMsg)

					completeMsg := protocol.GetCommandCompleteMessage("SET")
					connect.Send(client, completeMsg)

					readyMsg := protocol.GetTransactionOkMessage(false)
					connect.Send(client, readyMsg)

					commandBlock = false
				}
				continue
			}

			if annotations[PickSession] != "" && protocol.IsVertica(clientProtocolVersion) && dedicatedPool != nil {

				if requestType == protocol.ParseMessageType {
					bindMsg := protocol.GetNeedSimpleRequestMessage()
					completeMsg := protocol.GetParseCompleteMessage()
					connect.Send(client, append(bindMsg, completeMsg...))

				} else if requestType == protocol.QueryMessageType {

					statusMsg := protocol.GetParameterStatusMessage("session_set", annotations[PickSession])
					connect.Send(client, statusMsg)

					p.releaseConnection(currentPool, backendRecord, clientName)
					currentPool = nil
					backend = nil
					backendRecord = pool.Connection{}
					holdConnection = false
					ok := p.pickSession(userName, annotations[PickSession], dedicatedPool)

					var completeMsg []byte
					if ok {
						log.Infof("%s: session swithed to %s", clientName, annotations[PickSession])
						completeMsg = protocol.GetCommandCompleteMessage("SESSION PICKED")
					} else {
						log.Infof("%s: session %s not found", clientName, annotations[PickSession])
						completeMsg = protocol.GetCommandCompleteMessage("SESSION NOT FOUND")
					}

					connect.Send(client, completeMsg)

					readyMsg := protocol.GetTransactionOkMessage(false)
					connect.Send(client, readyMsg)

					commandBlock = false
				}
				continue
			}

			if annotations[DetachSession] != "" && protocol.IsVertica(clientProtocolVersion) && dedicatedPool != nil {

				if requestType == protocol.ParseMessageType {
					bindMsg := protocol.GetNeedSimpleRequestMessage()
					completeMsg := protocol.GetParseCompleteMessage()
					connect.Send(client, append(bindMsg, completeMsg...))

				} else if requestType == protocol.QueryMessageType {

					log.Infof("%s: session detach %s", clientName, annotations[DetachSession])

					if dedicatedPool.Len() > 0 {
						con := dedicatedPool.Pop()
						con.Detached = (annotations[DetachSession] == "on")
						dedicatedPool.Push(con)
					} else if backend == dedicatedBackend {
						backendRecord.Detached = (annotations[DetachSession] == "on")
					}

					statusMsg := protocol.GetParameterStatusMessage("session_detached", annotations[DetachSession])
					connect.Send(client, statusMsg)

					completeMsg := protocol.GetCommandCompleteMessage("SESSION DETACHED")
					connect.Send(client, completeMsg)

					readyMsg := protocol.GetTransactionOkMessage(false)
					connect.Send(client, readyMsg)

					commandBlock = false
				}
				continue
			}

			/* fetch a new backend to comminicate with */
			if currentPool == nil || backend == nil {
				currentPool = p.popPool(annotations, dedicatedPool)
				if currentPool.Len() == 0 {
					p.pushPool(currentPool, annotations)
					poolName = currentPool.Name
					currentPool = nil
					backend = nil
					backendRecord = pool.Connection{}

					log.Errorf("%s: no connections left in pool %s", clientName, poolName)
					pgError := protocol.Error{
						Severity: protocol.ErrorSeverityError,
						Code:     protocol.ErrorCodeTooManyConnections,
						Message:  "No connections left. Try again later",
					}
					connect.Send(client, pgError.GetMessage())

					if commandBlock {
						tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
						log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
						if err != nil {
							return
						}
						commandBlock = false
					}
					readyMsg := protocol.GetTransactionOkMessage(false)
					connect.Send(client, readyMsg)

					continue
				} else {
					poolName = currentPool.Name
					backendRecord = currentPool.Pop()
					backend = backendRecord.Con
					p.pushPool(currentPool, annotations)

					if currentPool == dedicatedPool {
						dedicatedBackend = backend
						dedicatedDetached = backendRecord.Detached
						clientRecord.CleintPid = backendRecord.BackendPid
						clientRecord.CleintSecret = backendRecord.BackendSecret
					}

					log.Debugf("%s: Backend %s locked (%d %d), %d connections left in pool %s", clientName,
						backendRecord.Name, backendRecord.BackendPid, backendRecord.BackendSecret,
						currentPool.Len(), poolName)
				}
			}

			/* Update the query count for the node being used. */
			p.lock.Lock()
			p.Stats[poolName] += 1
			backendRecord.Ts = time.Now()
			clientRecord.BackendPid = backendRecord.BackendPid
			clientRecord.BackendSecret = backendRecord.BackendSecret
			clientRecord.BackendRole = backendRecord.Role
			clientRecord.BackendName = backendRecord.Name
			clientRecord.IsRunning = true
			p.clients[clientName] = clientRecord
			p.lock.Unlock()

			protocolVersion := protocol.ParseProtocolVersionString(config.GetCredentials(backendRecord.Role).Protocol)

			/* set session autocommit for vertica connection only */
			if autocommit != backendRecord.Autocommit && annotations[DMLAnnotation] != "" &&
				protocol.IsVertica(protocolVersion) && config.GetPoolEnable() {
				var autocommitQuery string
				if autocommit {
					autocommitQuery = "set session autocommit to on"
				} else {
					autocommitQuery = "set session autocommit to off"
				}

				autocommitMsg := protocol.GetQueryMessage(autocommitQuery)
				if _, err = connect.Send(backend, autocommitMsg); err != nil {
					log.Debugf("%s: Error sending message to backend %s", clientName, backend.RemoteAddr())
					log.Debugf("%s: Error: %s", clientName, err.Error())
				} else {
					log.Debugf("%s: pending %s", clientName, autocommitQuery)
					backendRecord.Autocommit = autocommit
					tail, tailLength, _ := connect.RecievePendingMessages(backend)
					log.Infof("%s: flushing response: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
				}
			}

			/* patch query for postgres */
			if protocol.IsPG(protocolVersion) && protocol.IsVertica(clientProtocolVersion) {

				messageType := protocol.GetMessageType(message[queryStart:])
				messageLength := protocol.GetMessageLength(message[queryStart:])
				valid, patchMsg, patchMsgLength := protocol.PatchRequest(messageType, messageLength, message, queryStart, length)
				if valid && patchMsgLength > 0 {

					patchMsg = append(patchMsg[:int(patchMsgLength)+1], message[queryStart+int(messageLength)+1:length]...)
					message = append(message[:queryStart], patchMsg...)
					length += int(patchMsgLength - messageLength)
				}
			}

			/* Relay message to backend */
			if _, err = connect.Send(backend, message[:length]); err != nil {
				log.Debugf("%s: Error sending message to backend %s", clientName, backend.RemoteAddr())
				log.Debugf("%s: Error: %s", clientName, err.Error())
			}

			/* Continue to read from the backend until a 'ReadyForQuery' message is found. */
			responseType := byte(0)
			pledgedStart := 0
			protocolError := false
			pendingResponse = !waitingForResponse

			for waitingForResponse && !done {

				// log.Debugf("%s: Waiting for response", clientName)

				if message, length, err = connect.Receive(backend, 4096*8); err != nil {
					log.Errorf("%s: Error receiving response from backend %s", clientName, backend.RemoteAddr())
					log.Errorf("%s: Error: %s", clientName, err.Error())

					pgError := protocol.Error{
						Severity: protocol.ErrorSeverityError,
						Code:     protocol.ErrorCodeConnectionFailure,
						Message:  "Failed to receive response from backend: " + err.Error(),
					}
					connect.Send(client, pgError.GetMessage())

					if commandBlock {
						tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
						log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
						if err != nil {
							return
						}
						commandBlock = false
					}
					readyMsg := protocol.GetTransactionOkMessage(false)
					connect.Send(client, readyMsg)

					backendRecord.IsOk = false
					holdConnection = false
					break
				}

				// log.Debugf("%s: Parsing response", clientName)

				start := pledgedStart
				for start < length {

					// fix broken message header
					if start+1 > length-4 {
						delta := start + 1 - length + 4
						tail, tailLength, _ := connect.ReceiveExact(backend, delta)
						message = append(message[:length], tail[:tailLength]...)
						length += tailLength
					}

					responseType = protocol.GetMessageType(message[start:])
					messageLength := protocol.GetMessageLength(message[start:])

					// read broken message body
					if start+1+int(messageLength) > length {
						tail, tailLength, _ := connect.ReceiveExact(backend, start+1+int(messageLength)-length)
						message = append(message[:length], tail[:tailLength]...)
						length += tailLength
					}

					if responseType != protocol.DataRowMessageType {
						log.Debugf("%s: Response message: %c", clientName, responseType)
					}

					// patch pg response
					if protocol.IsPG(protocolVersion) && protocol.IsVertica(clientProtocolVersion) {

						valid, patchMsg, patchMsgLength, pathMsgType := protocol.PatchResponse(responseType, messageLength, message, start, length, backendRecord.Autocommit, anonPrepareIssued, describeIssued)
						if valid && patchMsgLength > 0 {

							patchMsg = append(patchMsg[:int(patchMsgLength)+1], message[start+int(messageLength)+1:length]...)
							message = append(message[:start], patchMsg...)
							length += int(patchMsgLength - messageLength)
							messageLength = patchMsgLength
							responseType = pathMsgType

							// if responseType != protocol.DataRowMessageType {
							// 	log.Debugf("%s: Patched  message: %s", clientName, log.GetReadableDatagram(message[start:length], 1 + int(messageLength)))
							// }
						} else if !valid && !protocolError {
							log.Debugf("Failed to patch datagram: %d", message[start:length])

							protocolError = true
							pgError := protocol.Error{
								Severity: protocol.ErrorSeverityError,
								Code:     protocol.ErrorCodeProtocolViolation,
								Message:  "Failed to decode/encode datagram",
							}
							connect.Send(client, pgError.GetMessage())

							if commandBlock {
								tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
								log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
								if err != nil {
									return
								}
								commandBlock = false
							}
							readyMsg := protocol.GetTransactionOkMessage(false)
							connect.Send(client, readyMsg)
						}
					}

					start = (start + int(messageLength) + 1)
				}

				// log.Debugf("%s: Routing response to client", clientName)

				if !protocolError {
					if _, err = connect.Send(client, message[:length]); err != nil {
						log.Errorf("%s: Error sending response to client", clientName)
						log.Errorf("%s: Error: %s", clientName, err.Error())
						protocolError = true
					}
				}

				if start >= length {
					pledgedStart = start - length
				} else {
					pledgedStart = 0
				}

				done = (responseType == protocol.ReadyForQueryMessageType ||
					requestType == protocol.ParseMessageType && responseType == protocol.ParseCompleteMessageType ||
					requestType == protocol.ParseMessageType && responseType == protocol.NeedRequestMessageType ||
					requestType == protocol.BindMessageType && responseType == protocol.BindCompleteMessageType ||
					requestType == protocol.CloseMessageType && responseType == protocol.CloseCompleteMessageType ||
					requestType == protocol.QueryMessageType && responseType == protocol.CopyInResponseMessageType ||
					requestType == protocol.CopyHeaderMessageType && responseType == protocol.FlushMessageType ||
					flushIssued && (responseType == protocol.ErrorMessageType || responseType == protocol.ParseCompleteMessageType ||
						responseType == protocol.BindCompleteMessageType || responseType == protocol.CloseCompleteMessageType))
			}

			log.Infof("%s: Client request served", clientName)

			/* update transaction state */
			if requestType == protocol.QueryMessageType && responseType == protocol.ReadyForQueryMessageType && namedPrepare == "" ||
				requestType == protocol.SyncMessageType && responseType == protocol.ReadyForQueryMessageType && namedPrepare == "" ||
				requestType == protocol.CopyDoneMessageType && responseType == protocol.ReadyForQueryMessageType && namedPrepare == "" ||
				responseType == protocol.CloseCompleteMessageType && namedPrepare == "" ||
				requestType == protocol.CloseMessageType && !waitingForResponse && !config.GetPoolEnable() {

				commandBlock = false
			}

			if !commandBlock {
				clientRecord.StatementNo += 1
			}

			/* release connection if it is possible */
			if !commandBlock && !(holdConnection && config.GetPoolEnable()) || !backendRecord.IsOk {
				p.releaseConnection(currentPool, backendRecord, clientName)
				currentPool = nil
				backend = nil
				backendRecord = pool.Connection{}
			} else {
				p.lock.Lock()
				clientRecord, ok := p.clients[clientName]
				if ok {
					clientRecord.IsRunning = false
					p.clients[clientName] = clientRecord
				}
				p.lock.Unlock()
			}

		} else {
			log.Debugf("%s: Message type %c not supported", clientName, requestType)

			pgError := protocol.Error{
				Severity: protocol.ErrorSeverityError,
				Code:     protocol.ErrorCodeFeatureNotSupported,
				Message:  "Only simple query protocol supported",
			}
			connect.Send(client, pgError.GetMessage())

			if commandBlock {
				tail, tailLength, err := connect.ReceiveTimeout(client, responseTimeout)
				log.Infof("%s: flushing transaction sync: %s", clientName, log.GetReadableDatagram(tail[:tailLength], tailLength))
				if err != nil {
					return
				}
				commandBlock = false
			}
			readyMsg := protocol.GetTransactionOkMessage(false)
			connect.Send(client, readyMsg)
		}
	}
}
