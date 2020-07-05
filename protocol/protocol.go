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
	"io"
	"bytes"
	"strconv"
	"strings"
	"regexp"
	"encoding/binary"
)

/* PostgreSQL Protocol Version/Code constants */
const (
	PGProtocolVersion int32 = 0x30000 //196608
	VProtocolVersion int32 = 0x30008 //196616
	ProtocolVersion int32 = VProtocolVersion

	IsVerticaSupported bool = (ProtocolVersion == VProtocolVersion)

	SSLRequestCode  int32 = 80877103
	CancelRequest   int32 = 80877102

	/* SSL Responses */
	SSLAllowed    byte = 'S'
	SSLNotAllowed byte = 'N'
)

/* PostgreSQL Message Type constants. */
const (
	AuthenticationMessageType   byte = 'R'
	ErrorMessageType            byte = 'E'
	EmptyQueryMessageType       byte = 'I'
	DescribeMessageType         byte = 'D'
	RowDescriptionMessageType   byte = 'T'
	ParamDescriptionMessageType byte = 't'
	DataRowMessageType          byte = 'D'
	QueryMessageType            byte = 'Q'
	CommandCompleteMessageType  byte = 'C'
	TerminateMessageType        byte = 'X'
	NoticeMessageType           byte = 'N'
	PasswordMessageType         byte = 'p'
	ReadyForQueryMessageType    byte = 'Z'
	ParseMessageType            byte = 'P'
	ParseCompleteMessageType    byte = '1'
	BindMessageType             byte = 'B'
	BindCompleteMessageType     byte = '2'
	CloseMessageType            byte = 'C'
	CloseCompleteMessageType    byte = '3'
	BackendKeyDataMessageType   byte = 'K'
	NeedRequestMessageType      byte = 'm'
	NoDataMessageType           byte = 'n'
	SyncMessageType             byte = 'S'
	ParameterStatusMessageType  byte = 'S'
	FlushMessageType            byte = 'H'
	CopyInResponseMessageType   byte = 'F'
	CopyHeaderMessageType       byte = 'F'
	CopyErrorMessageType        byte = 'e'
	CopyDataMessageType         byte = 'd'
	CopyDataSentMessageType     byte = 'j'
	CopyDataReceivedMessageType byte = 'J'
	CopyDoneMessageType         byte = 'c'
)

/* PostgreSQL Authentication Method constants. */
const (
	AuthenticationOk          int32 = 0
	AuthenticationKerberosV5  int32 = 2
	AuthenticationClearText   int32 = 3
	AuthenticationMD5         int32 = 5
	AuthenticationSCM         int32 = 6
	AuthenticationGSS         int32 = 7
	AuthenticationGSSContinue int32 = 8
	AuthenticationSSPI        int32 = 9
	AuthenticationMAX         int32 = 10
)

func IsPG(version int32) bool {
	return version <= 0x30003
}

func IsVertica(version int32) bool {
	return version >= 0x30006
}

func GetStartupProtocol(message []byte) int32 {
	var code int32

	reader := bytes.NewReader(message[4:8])
	binary.Read(reader, binary.BigEndian, &code)

	return code
}

func GetStartupClientCredits(message []byte) (string,string,string,string) {
	var clientUser string
	var clientDatabase string
	var clientType string
	var clientVersion string
	var clientLabel string

	startup := NewMessageBuffer(message)

	startup.Seek(8) // Seek past the message length and protocol version.

	for {
		param, err := startup.ReadString()

		if err == io.EOF || param == "\x00" {
			break
		}

		switch param {
		case "user":
			clientUser, err = startup.ReadString()
		case "database":
			clientDatabase, err = startup.ReadString()
		case "client_type":
			clientType, err = startup.ReadString()
		case "client_version":
			clientVersion, err = startup.ReadString()
		case "client_label":
			clientLabel, err = startup.ReadString()
		}
	}

	if clientVersion == "" && clientLabel != "" {
		re := regexp.MustCompile("\\d+\\.\\d+\\.\\d+")
		clientVersion = re.FindString(clientLabel)
	}

	return clientUser, clientDatabase, clientType, clientVersion
}

/*
 * Get the message type the provided message.
 *
 * message - the message
 */
func GetMessageType(message []byte) byte {
	return message[0]
}

/*
 * Get the message length of the provided message.
 *
 * message - the message
 */
func GetMessageLength(message []byte) int32 {
	var messageLength int32

	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &messageLength)

	return messageLength
}

func GetTerminateMessage() []byte {
	var buffer []byte
	buffer = append(buffer, TerminateMessageType)

	//make msg len 1 for now
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4))
	buffer = append(buffer, x...)
	return buffer
}

func GetReadyForQueryMessage() []byte {
	var buffer []byte
	buffer = append(buffer, ReadyForQueryMessageType)
	
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(5))
	buffer = append(buffer, x...)

	buffer = append(buffer, 'I')

	return buffer
}

func GetTransactionOkMessage(inTransaction bool) []byte {
	var buffer []byte
	buffer = append(buffer, ReadyForQueryMessageType)
	
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(5))
	buffer = append(buffer, x...)

	if inTransaction {
		buffer = append(buffer, 'T') 
	} else {
		buffer = append(buffer, 'I') 
	}

	return buffer
}

func GetCancelRequest(pid int32, secret int32) []byte {
	msg := NewMessageBuffer([]byte{})

	msg.WriteInt32(16)
	msg.WriteInt32(CancelRequest)
	msg.WriteInt32(pid)
	msg.WriteInt32(secret)

	return msg.Bytes()
}

func GetParameterStatusMessage(parameter string, value string) [] byte {

	msg := NewMessageBuffer([]byte{})

	msg.WriteByte(ParameterStatusMessageType)
	msg.WriteInt32(int32(4 + len(parameter) + 1 + len(value) + 1))
	msg.WriteString(parameter)
	msg.WriteString(value)

	return msg.Bytes()
}

func GetNeedSimpleRequestMessage() []byte {
	var buffer []byte
	buffer = append(buffer, NeedRequestMessageType)

	m := []byte("SELECT")

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4 + len(m) + 4))
	buffer = append(buffer, x...)

	buffer = append(buffer, m[:len(m)]...)

	x = make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(0))
	buffer = append(buffer, x...)

	return buffer
}

func GetNoDataMessage() []byte {
	var buffer []byte
	buffer = append(buffer, NoDataMessageType)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4))
	buffer = append(buffer, x...)
	return buffer
}

func GetParseCompleteMessage() []byte {
	var buffer []byte
	buffer = append(buffer, ParseCompleteMessageType)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4))
	buffer = append(buffer, x...)
	return buffer
}

// CloseComplete
//ve: 67 0 0 0 5 0
//pg: 67 0 0 0 13  S  E  L  E  C  T     1 0
//pg: 67 0 0 0 13 83 69 76 69 67 84 32 49 0

func GetCommandCompleteMessage(res string) []byte {
	var buffer []byte
	buffer = append(buffer, CommandCompleteMessageType)
	
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4 + len(res) + 1))
	buffer = append(buffer, x...)

	buffer = append(buffer, []byte(res)...)
	buffer = append(buffer, byte(0))

	return buffer
}

func GetQueryMessage(query string) []byte {
	var buffer []byte
	buffer = append(buffer, QueryMessageType)
	
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4 + len(query) + 1))
	buffer = append(buffer, x...)

	buffer = append(buffer, []byte(query)...)
	buffer = append(buffer, byte(0))

	return buffer
}

func GetParseMessage(name string, query string) []byte {
	var buffer []byte
	buffer = append(buffer, ParseMessageType)
	
	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4 + len(name) + 1 + len(query) + 1 + 2))
	buffer = append(buffer, x...)

	buffer = append(buffer, []byte(name)...)
	buffer = append(buffer, byte(0))

	buffer = append(buffer, []byte(query)...)
	buffer = append(buffer, byte(0))

	buffer = append(buffer, byte(0))
	buffer = append(buffer, byte(0))

	return buffer
}

func GetSyncMessage() []byte {
	var buffer []byte
	buffer = append(buffer, SyncMessageType)

	x := make([]byte, 4)
	binary.BigEndian.PutUint32(x, uint32(4))
	buffer = append(buffer, x...)
	return buffer
}

func ParseProtocolVersionString(protocolVersion string) int32 {
	var ver int32
	v, err := strconv.ParseInt(protocolVersion, 0, 32)
    if err != nil {
        //log.Infof("Failed to parse protocol version %s", protocolVersion)
        ver = ProtocolVersion
    } else {
    	ver = int32(v)
    }
    return ver
}

func ParseClientVersionString(clientVersion string) int32 {
	if match, _ := regexp.MatchString("^\\d+\\.\\d+\\.\\d+$", clientVersion); match {
		parts := strings.Split(clientVersion, ".")
		major,_ := strconv.Atoi(parts[0])
		minor,_ := strconv.Atoi(parts[1])
		patch,_ := strconv.Atoi(parts[2])

		return int32(patch) | int32(minor << 8) | int32(major << 16)
	}
	return 0
}

func ParseName(message []byte, length int) string {
	var name string

	b := NewMessageBuffer(message[:length])
	messageType, _ := b.ReadByte()
	messageLength, _ := b.ReadInt32()
	if messageLength >= 5 && messageType == ParseMessageType {
		name, _ = b.ReadString()
	} else if messageLength >= 6 && messageType == CloseMessageType {
		b.ReadByte()
		name, _ = b.ReadString()
	} else if messageLength >= 6 && messageType == DescribeMessageType {
		b.ReadByte()
		name, _ = b.ReadString()
	} 

	return name
}