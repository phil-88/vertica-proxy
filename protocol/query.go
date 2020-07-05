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
	//"regexp"
	"github.com/crunchydata/crunchy-proxy/util/log"
	"strings"
	"strconv"
)

const (
	PGbool        int32 = 16
	PGbytea       int32 = 17
	PGchar        int32 = 18
	PGint8        int32 = 20
	PGint2        int32 = 21
	PGint4        int32 = 23
	PGtext        int32 = 25
	PGfloat4      int32 = 700
	PGfloat8      int32 = 701
	PGunknown     int32 = 705
	PGvarchar     int32 = 1043
	PGdate        int32 = 1082
	PGtime        int32 = 1083
	PGtimestamp   int32 = 1114
	PGtimestamptz int32 = 1184
	PGnumeric     int32 = 1700
)

const (
	Vint         int32 =  6
    Vnumeric     int32 = 16
    Vvarchar     int32 =  9
    Vbool        int32 =  5
    Vfloat       int32 =  7
    Vchar        int32 =  8
    Vdate        int32 = 10
    Vtime        int32 = 11
    Vtimestamp   int32 = 12
    Vtimestamptz int32 = 12
)

type Description struct {
	Title  string
	TypeId int32
}

type BindValue struct {
	Fmt int16
	TypeId int32
	Len int32
	Val []byte
}

func ParsePGRowDescription(message []byte, length int) []Description {
	var cols []Description 

	msg := NewMessageBuffer(message)
	msg.Seek(5)
	
	count, _ := msg.ReadInt16()
	for i := int16(0); i < count; i += 1 {
		Title, _ := msg.ReadString()
		msg.Seek(6)
		TypeId, _ := msg.ReadInt32()
		msg.Seek(8)

		column := Description{
			Title: Title,
			TypeId: int32(TypeId),
		}
		
		cols = append(cols, column)
	}

	return cols
}

func ParsePGParamDescription(message []byte, length int) []Description {
	var cols []Description 

	msg := NewMessageBuffer(message)
	msg.Seek(5)
	
	count, _ := msg.ReadInt16()
	for i := int16(0); i < count; i += 1 {
		TypeId, _ := msg.ReadInt32()

		column := Description{
			Title: "",
			TypeId: int32(TypeId),
		}
		
		cols = append(cols, column)
	}

	return cols
}

// B 0 0 0 27 0 s2 0 0 1 0 0 0 1 0 0 0 9 0 0 0 3 DMA 0 0 

func ParseBind(message []byte, length int) (string,string,[]BindValue) {
	var fmts []int16
	var types []int32
	var values []BindValue 

	msg := NewMessageBuffer(message)
	msg.Seek(5)

	portal, _ := msg.ReadString()
	stmt, _ := msg.ReadString()

	fmtCnt, _ := msg.ReadInt16()
	for i := int16(0); i < fmtCnt; i++ {
		fmt, _ := msg.ReadInt16()
		fmts = append(fmts, fmt)
	}
	
	valCnt, _ := msg.ReadInt16()
	for i := int16(0); i < valCnt; i += 1 {
		TypeId , _ := msg.ReadInt32()
		types = append(types, TypeId)
	}

	for i := int16(0); i < valCnt; i += 1 {
		Len, _ := msg.ReadInt32()
		Val, _ := msg.ReadBytes(int(Len))

		Fmt := int16(0)
		if fmtCnt == 1 {
			Fmt = fmts[0]
		} else if valCnt == fmtCnt {
			Fmt = fmts[i]
		}
		
		TypeId := types[i]

		value := BindValue{
			Fmt: Fmt,
			TypeId: TypeId,
			Len: Len,
			Val: Val,
		}
		
		values = append(values, value)
	}

	return portal, stmt, values
}

func GetPGBind(portal string, stmt string, values []BindValue) (bool,[]byte,int32) {
	ok := true
	msg := NewMessageBuffer([]byte{})

	msg.WriteByte(BindMessageType)
	msg.WriteInt32(0)

	msg.WriteString(portal)
	msg.WriteString(stmt)

	msg.WriteInt16(int16(len(values)))
	for _, value := range values {
		msg.WriteInt16(value.Fmt)
	}
	
	msg.WriteInt16(int16(len(values)))
	for _, value := range values {
		msg.WriteInt32(value.Len)
		msg.WriteBytes(value.Val[:value.Len])	
	}

	msg.WriteInt16(int16(0))

	msg.ResetLength(1)
	buffer := msg.Bytes()

	return ok, buffer, int32(len(buffer) - 1)
}

// RowDescription
//vertica int        6   8  -1
//        numeric   16  -1   0 18 0 4
//        varchar    9  -1   4 + maxlen
//        bool       5   1  -1
//        float      7   8  -1
//        char       8  -1   4 + maxlen
//        date      10   8  -1
//        time      11   8  -1
//        timestamp 12   8  -1

func GetRowDescriptionFromPG(cols []Description) (bool,[]byte,int32) {
	ok := true
	msg := NewMessageBuffer([]byte{})

	msg.WriteByte(RowDescriptionMessageType)
	msg.WriteInt32(0)

	msg.WriteInt16(int16(len(cols)))
	msg.WriteInt32(0)

	for _, column := range cols {
		msg.WriteBytes([]byte(column.Title[:len(column.Title)]))
		msg.WriteInt32(0)
		msg.WriteInt32(0)
		msg.WriteInt32(0)
		if column.TypeId == PGbool {
			msg.WriteInt32(Vbool)
			msg.WriteInt16(int16(1))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint8 {
			msg.WriteInt32(Vint)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint4 {
			msg.WriteInt32(Vint)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint2 {
			msg.WriteInt32(Vint)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))			
		} else if column.TypeId == PGvarchar || column.TypeId == PGchar || 
				  column.TypeId == PGtext || column.TypeId == PGbytea || 
				  column.TypeId == PGunknown {
			msg.WriteInt32(Vvarchar)
			msg.WriteInt16(int16(-1))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(4+4096))
		} else if column.TypeId == PGtimestamp {
			msg.WriteInt32(Vtimestamp)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGtimestamptz {
			msg.WriteInt32(Vtimestamptz)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGfloat8 {
			msg.WriteInt32(Vfloat)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGfloat4 {
			msg.WriteInt32(Vfloat)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGdate {
			msg.WriteInt32(Vdate)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGtime {
			msg.WriteInt32(Vtime)
			msg.WriteInt16(int16(8))
			msg.WriteInt32(int32(0x00010000))
			msg.WriteInt32(int32(-1))
		} else {
			msg.WriteInt32(int32(0))
			msg.WriteInt16(int16(0))
			msg.WriteInt32(int32(0))
			msg.WriteInt32(int32(0))
			ok = false
			log.Errorf("PG type not supported: %d", column.TypeId)
		}
		msg.WriteInt16(int16(0))
	}
	
	msg.ResetLength(1)
	buffer := msg.Bytes()

	return ok, buffer, int32(len(buffer) - 1)
}

func GetParamDescriptionFromPG(cols []Description) (bool,[]byte,int32) {
	ok := true
	msg := NewMessageBuffer([]byte{})

	msg.WriteByte(ParamDescriptionMessageType)
	msg.WriteInt32(0)

	msg.WriteInt16(int16(len(cols)))
	msg.WriteInt32(0)
	
	for _, column := range cols {
		msg.WriteByte(0)
		if column.TypeId == PGbool {
			msg.WriteInt32(Vbool)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint8 {
			msg.WriteInt32(Vint)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint4 {
			msg.WriteInt32(Vint)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGint2 {
			msg.WriteInt32(Vint)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGvarchar || column.TypeId == PGchar || 
				  column.TypeId == PGtext || column.TypeId == PGbytea || 
				  column.TypeId == PGunknown {
			msg.WriteInt32(Vvarchar)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGtimestamp {
			msg.WriteInt32(Vtimestamp)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGtimestamptz {
			msg.WriteInt32(Vtimestamptz)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGfloat8 {
			msg.WriteInt32(Vfloat)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGfloat4 {
			msg.WriteInt32(Vfloat)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGdate {
			msg.WriteInt32(Vdate)
			msg.WriteInt32(int32(-1))
		} else if column.TypeId == PGtime {
			msg.WriteInt32(Vtime)
			msg.WriteInt32(int32(-1))
		} else {
			msg.WriteInt32(int32(0))
			msg.WriteInt32(int32(0))
			ok = false
			log.Errorf("PG type not supported: %d", column.TypeId)
		}
		msg.WriteInt16(int16(0))
	}
	
	msg.ResetLength(1)
	buffer := msg.Bytes()

	return ok, buffer, int32(len(buffer) - 1)
}

func PatchResponse(messageType byte, messageLength int32, message []byte, start int, length int, autocommit bool, respToAnonPrepare bool, respToDescribe bool) (bool,[]byte,int32,byte) {

	if int(messageLength) > length - start - 1 {
		return false, []byte{}, 0, messageType
	} 

	if messageType == RowDescriptionMessageType {

		cols := ParsePGRowDescription(message[start:length], length - start)
		ok, resp, respLength := GetRowDescriptionFromPG(cols)
		var respType byte = RowDescriptionMessageType

		var bindMsg []byte = []byte{}
		var bindMsgLength int32 = -1

		if respToDescribe {
			bindMsg = GetNeedSimpleRequestMessage()
			bindMsgLength = GetMessageLength(bindMsg)	
			respType = NeedRequestMessageType		
		}

		return ok, append(resp,bindMsg...), respLength + 1 + bindMsgLength, respType
	}

	if messageType == ParamDescriptionMessageType {

		cols := ParsePGParamDescription(message[start:length], length - start)
		ok, resp, respLength := GetParamDescriptionFromPG(cols)

		return ok,resp, respLength, messageType
	}

	if messageType == NoDataMessageType {

		resp := GetNoDataMessage()
		respLength := GetMessageLength(resp)
		var respType byte = NoDataMessageType

		var bindMsg []byte = []byte{}
		var bindMsgLength int32 = -1

		if respToDescribe {
			bindMsg = GetNeedSimpleRequestMessage()
			bindMsgLength = GetMessageLength(bindMsg)		
			respType = NeedRequestMessageType	
		}

		return true, append(resp,bindMsg...), respLength + 1 + bindMsgLength, respType
	}

	if messageType == CommandCompleteMessageType {

		resp := GetCommandCompleteMessage("")
		respLength := GetMessageLength(resp)

		return true, resp, respLength, messageType
	}

	if messageType == DataRowMessageType {
		return true, []byte{}, 0, messageType
	}

	if messageType == ReadyForQueryMessageType {
		readyMsg := GetTransactionOkMessage(!autocommit)
		readyMsgLen := GetMessageLength(readyMsg)
		return true, readyMsg, readyMsgLen, messageType
	} 

	if messageType == ErrorMessageType {
		pgError := ParseError(message)
		vError := Error{
			Severity: pgError.Severity,
			Code:     pgError.Code,
			Message:  pgError.Message,
		}
		msg := vError.GetMessage()
		msgLen := GetMessageLength(msg)
		return true, msg, msgLen, messageType
	}

	if messageType == ParseCompleteMessageType {

		var bindMsg []byte = []byte{}
		var bindMsgLength int32 = -1
		var respType byte = ParseCompleteMessageType

		if respToAnonPrepare {
			bindMsg = GetNeedSimpleRequestMessage()
			bindMsgLength = GetMessageLength(bindMsg)	
			respType = NeedRequestMessageType
		}

		completeMsg := GetParseCompleteMessage()
		completeMsgLength := GetMessageLength(completeMsg)

		return true, append(bindMsg,completeMsg...), bindMsgLength + 1 + completeMsgLength, respType
	} 

	if messageType == SyncMessageType {
		return true, []byte{}, 0, messageType
	}

	return true, []byte{}, 0, messageType
}

func PatchRequest(messageType byte, messageLength int32, message []byte, start int, length int) (bool,[]byte,int32) {

	if int(messageLength) > length - start - 1 {
		return false, []byte{}, 0
	} 

	if messageType == QueryMessageType {

		b := NewMessageBuffer(message[start:])
		b.ReadByte() 
		b.ReadInt32()
		query, _ := b.ReadString()

		// fix pg "failed to find conversion function from unknown to text"
		// re0 := regexp.MustCompile("([^\\\\])\\\\'")
		// query = re0.ReplaceAllString(query, "$1__ESCAPEDQUOTE__")

		// re := regexp.MustCompile("('[^']*')")
		// query = re.ReplaceAllString(query, "$1::varchar")

		// re2 := regexp.MustCompile("__ESCAPEDQUOTE__")
		// query = re2.ReplaceAllString(query, "\\'")

		// re3 := regexp.MustCompile("'([tf]|[0-9]+)'::varchar")
		// query = re3.ReplaceAllString(query, "'$1'")


		for i := 1; i < 10; i++ {
			query = strings.Replace(query, "?", "$" + strconv.Itoa(i), 1)
		}

		resp := GetQueryMessage(query)
		respLength := GetMessageLength(resp)
		return true, resp, respLength
	}

	if messageType == ParseMessageType {

		b := NewMessageBuffer(message[start:])
		b.ReadByte() 
		b.ReadInt32()
		name, _ := b.ReadString()
		query, _ := b.ReadString()

		// fix pg crap with "failed to find conversion function from unknown to text"
		// re0 := regexp.MustCompile("([^\\\\])\\\\'")
		// query = re0.ReplaceAllString(query, "$1__ESCAPEDQUOTE__")

		// re := regexp.MustCompile("('[^']*')")
		// query = re.ReplaceAllString(query, "$1::varchar")

		// re2 := regexp.MustCompile("__ESCAPEDQUOTE__")
		// query = re2.ReplaceAllString(query, "\\'")

		// re3 := regexp.MustCompile("'([tf]|[0-9]+)'::varchar")
		// query = re3.ReplaceAllString(query, "'$1'")

		for i := 1; i < 10; i++ {
			query = strings.Replace(query, "?", "$" + strconv.Itoa(i), 1)
		}

		resp := GetParseMessage(name, query)
		respLength := GetMessageLength(resp)
		return true, resp, respLength
	}

	if messageType == BindMessageType {
		portal, prepare, values := ParseBind(message[start:length], length - start)
		ok, resp, respLength := GetPGBind(portal, prepare, values)
		return ok,resp, respLength
	}

	return true, []byte{}, 0
}
