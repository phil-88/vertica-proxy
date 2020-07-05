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

package protocol

import (
	"bytes"
	"encoding/binary"
)

func CreatePasswordMessage(password string) []byte {
	message := NewMessageBuffer([]byte{})

	/* Set the message type */
	message.WriteByte(PasswordMessageType)

	/* Initialize the message length to zero. */
	message.WriteInt32(0)

	/* Add the password to the message. */
	message.WriteString(password)

	/* Update the message length */
	message.ResetLength(PGMessageLengthOffset)

	return message.Bytes()
}

/* IsAuthenticationOk
 *
 * Check an Authentication Message to determine if it is an AuthenticationOK
 * message.
 */
func IsAuthenticationOk(message []byte) bool {
	/*
	 * If the message type is not an Authentication message, then short circuit
	 * and return false.
	 */
	if GetMessageType(message) != AuthenticationMessageType {
		return false
	}

	var messageValue int32

	// Get the message length.
	messageLength := GetMessageLength(message)

	// Get the message value.
	reader := bytes.NewReader(message[5:9])
	binary.Read(reader, binary.BigEndian, &messageValue)

	return (messageLength == 8 && (messageValue == AuthenticationOk || messageValue >= AuthenticationMAX))
}

func GetAuthenticationMethod(message []byte) int32 {

	if GetMessageType(message) != AuthenticationMessageType {
		return -1
	}

	messageLength := GetMessageLength(message)
	if messageLength < 8 {
		return -1
	}

	var messageValue int32
	reader := bytes.NewReader(message[5:9])
	binary.Read(reader, binary.BigEndian, &messageValue)

	return messageValue
}

func GetAuthenticationOkMessage() []byte {
	var buffer []byte
	buffer = append(buffer, AuthenticationMessageType)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(8))
	buffer = append(buffer, x...)

	x = make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(0))
	buffer = append(buffer, x...)

	return buffer
}

func GetBackendKeyDataMessage(pid int32, secret int32) []byte {
	var buffer []byte
	buffer = append(buffer, BackendKeyDataMessageType)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(12))
	buffer = append(buffer, x...)

	x = make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(pid))
	buffer = append(buffer, x...)

	x = make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(secret))
	buffer = append(buffer, x...)

	return buffer
}

func GetBackendPid(message []byte) int32 {
	var code int32

	if message[0] == BackendKeyDataMessageType {
		reader := bytes.NewReader(message[5:9])
		binary.Read(reader, binary.BigEndian, &code)
	}

	if GetStartupProtocol(message) == CancelRequest {
		reader := bytes.NewReader(message[8:12])
		binary.Read(reader, binary.BigEndian, &code)
	}

	return code
}

func GetBackendSecret(message []byte) int32 {
	var code int32

	if message[0] == BackendKeyDataMessageType {
		reader := bytes.NewReader(message[9:13])
		binary.Read(reader, binary.BigEndian, &code)
	}

	if GetStartupProtocol(message) == CancelRequest {
		reader := bytes.NewReader(message[12:16])
		binary.Read(reader, binary.BigEndian, &code)
	}

	return code
}
