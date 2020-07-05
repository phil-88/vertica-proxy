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

package connect

import (
	"net"
	"bytes"
	"time"
	"encoding/binary"

	"github.com/crunchydata/crunchy-proxy/config"
	"github.com/crunchydata/crunchy-proxy/protocol"
	"github.com/crunchydata/crunchy-proxy/util/log"
)

func Send(connection net.Conn, message []byte) (int, error) {
	return connection.Write(message)
}

func Receive(connection net.Conn, size int) ([]byte, int, error) {
	/* Received chunk is inconsistent on logical level of PQ */
	buffer := make([]byte, size)
	length, err := connection.Read(buffer)
	return buffer, length, err
}

func ReceiveTimeout(connection net.Conn, timeout time.Duration) ([]byte, int, error) {
	/* Received chunk is inconsistent on logical level of PQ */
	connection.SetReadDeadline(time.Now().Add(timeout))
	buffer, length, err := RecieveSingleMessage(connection)
	connection.SetReadDeadline(time.Time{})

	if err != nil {
        netErr, ok := err.(net.Error)
        if ok && netErr.Timeout() && length == 0 {
        	return buffer, 0, nil
        }
    }
	
	return buffer, length, err
}

func ReceiveExact(connection net.Conn, size int) ([]byte, int, error) {
	var msg []byte
	var length int = 0
	var err error

	for length < size {
		chunk, chunkLength, err := Receive(connection, size - length)
		if err != nil {
			return msg, length, err
		}
		msg = append(msg, chunk[:chunkLength]...)
		length += chunkLength
	}

	return msg, length, err
}

func RecieveSingleMessage(connection net.Conn) ([]byte, int, error) {
	header, headerLength, err := ReceiveExact(connection, 5)
	if err != nil {
		return header, headerLength, err
	}

	msgLength := protocol.GetMessageLength(header)
	body, bodyLength, err := ReceiveExact(connection, int(msgLength) - 4)
	return append(header, body...), bodyLength + 5, err
}

func RecievePendingMessages(connection net.Conn) ([]byte, int, error) {

	var fullMessage []byte
	var fullLength int = 0
	var err error

	var message []byte
	var length int
	//var messageType byte = 0
	var pledgedStart int = 0
	var done bool = false

	for !done {
		
		if message, length, err = Receive(connection, 4096); err != nil {
			break
		}

		start := pledgedStart
		for start < length {

			// fix broken message header
			if start + 1 > length - 4 {
				delta := start + 1 - length + 4
				tail, tailLength, _ := ReceiveExact(connection, delta)
				message = append(message[:length], tail[:tailLength]...)
				length += tailLength
			}

			//messageType = protocol.GetMessageType(message[start:])
			messageLength := protocol.GetMessageLength(message[start:])

			start = (start + int(messageLength) + 1)
		}

		if start >= length {
			pledgedStart = start - length
		} else {
			pledgedStart = 0
		}

		fullMessage = append(fullMessage, message[:length]...)
		fullLength += length

		done = (length == start) // messageType == protocol.FlushMessageType
	}

	return fullMessage, fullLength, err
}

func RecieveStartupMessage(connection net.Conn) ([]byte, int, error) {
	var header []byte
	var headerLength int
	var err error

	header, headerLength, err = ReceiveExact(connection, 4)
	if err != nil {
		return header, headerLength, err
	}

	var msgLength int32
	reader := bytes.NewReader(header[0:4])
	binary.Read(reader, binary.BigEndian, &msgLength)

	var body []byte
	var bodyLength int

	body, bodyLength, err = ReceiveExact(connection, int(msgLength) - 4)
	if err != nil {
		return header, headerLength, err
	}
	
	return append(header, body...), headerLength + bodyLength, err
}

func Connect(host string, role string) (net.Conn, error) {
	connection, err := net.Dial("tcp", host)

	if err != nil {
		return nil, err
	}

	cred := config.GetCredentials(role)

	if cred.SSL.Enable {
		log.Info("SSL connections are enabled.")

		/*
		 * First determine if SSL is allowed by the backend. To do this, send an
		 * SSL request. The response from the backend will be a single byte
		 * message. If the value is 'S', then SSL connections are allowed and an
		 * upgrade to the connection should be attempted. If the value is 'N',
		 * then the backend does not support SSL connections.
		 */

		/* Create the SSL request message. */
		message := protocol.NewMessageBuffer([]byte{})
		message.WriteInt32(8)
		message.WriteInt32(protocol.SSLRequestCode)

		/* Send the SSL request message. */
		_, err := connection.Write(message.Bytes())

		if err != nil {
			log.Error("Error sending SSL request to backend.")
			log.Errorf("Error: %s", err.Error())
			return nil, err
		}

		/* Receive SSL response message. */
		response := make([]byte, 4096)
		_, err = connection.Read(response)

		if err != nil {
			log.Error("Error receiving SSL response from backend.")
			log.Errorf("Error: %s", err.Error())
			return nil, err
		}

		/*
		 * If SSL is not allowed by the backend then close the connection and
		 * throw an error.
		 */
		if len(response) > 0 && response[0] != 'S' {
			log.Error("The backend does not allow SSL connections.")
			connection.Close()
		} else {
			log.Debug("SSL connections are allowed by PostgreSQL.")
			log.Debug("Attempting to upgrade connection.")
			connection = UpgradeClientConnection(host, connection, role)
			log.Debug("Connection successfully upgraded.")
		}
	}

	return connection, nil
}
