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
	"fmt"
)

/* PG Error Severity Levels */
const (
	ErrorSeverityError   string = "ERROR"
	ErrorSeverityFatal   string = "FATAL"
	ErrorSeverityPanic   string = "PANIC"
	ErrorSeverityWarning string = "WARNING"
	ErrorSeverityNotice  string = "NOTICE"
	ErrorSeverityDebug   string = "DEBUG"
	ErrorSeverityInfo    string = "INFO"
	ErrorSeverityLog     string = "LOG"
)

/* PG Error Message Field Identifiers */
const (
	ErrorFieldSeverity         byte = 'S'
	ErrorFieldCode             byte = 'C'
	ErrorFieldMessage          byte = 'M'
	ErrorFieldMessageDetail    byte = 'D'
	ErrorFieldMessageHint      byte = 'H'
	ErrorFieldPosition         byte = 'P'
	ErrorFieldInternalPosition byte = 'p'
	ErrorFieldInternalQuery    byte = 'q'
	ErrorFieldWhere            byte = 'W'
	ErrorFieldSchemaName       byte = 's'
	ErrorFieldTableName        byte = 't'
	ErrorFieldColumnName       byte = 'c'
	ErrorFieldDataTypeName     byte = 'd'
	ErrorFieldConstraintName   byte = 'n'
	ErrorFieldFile             byte = 'F'
	ErrorFieldLine             byte = 'L'
	ErrorFieldRoutine          byte = 'R'
)

type Error struct {
	Severity         string
	Code             string
	Message          string
	Detail           string
	Hint             string
	Position         string
	InternalPosition string
	InternalQuery    string
	Where            string
	SchemaName       string
	TableName        string
	ColumnName       string
	DataTypeName     string
	Constraint       string
	File             string
	Line             string
	Routine          string
}

func (e *Error) Error() string {
	return fmt.Sprintf("pg: %s: %s", e.Severity, e.Message)
}

func (e *Error) GetMessage() []byte {

	//E`SERRORC42703V2624MColumn "hello" does not existFparse_expr.cL1375RtransformColumnRef
	//[69 0 0 0 96 83 69 82 82 79 82 0 67 52 50 55 48 51 0 86 50 54 50 52 0 77 67 111 108 117 109 110 32 34 104 101 108 108 111 34 32 100 111 101 115 32 110 111 116 32 101 120 105 115 116 0 70 112 97 114 115 101 95 101 120 112 114 46 99 0 76 49 51 55 53 0 82 116 114 97 110 115 102 111 114 109 67 111 108 117 109 110 82 101 102 0 0]
	//ZT
	//[90 0 0 0 5 84]

	msg := NewMessageBuffer([]byte{})

	msg.WriteByte(ErrorMessageType)
	msg.WriteInt32(0)

	msg.WriteByte(ErrorFieldSeverity)
	msg.WriteString(e.Severity)

	msg.WriteByte(ErrorFieldCode)
	msg.WriteString(e.Code)

	msg.WriteByte(ErrorFieldMessage)
	msg.WriteString(e.Message)

	if e.Detail != "" {
		msg.WriteByte(ErrorFieldMessageDetail)
		msg.WriteString(e.Detail)
	}

	if e.Hint != "" {
		msg.WriteByte(ErrorFieldMessageHint)
		msg.WriteString(e.Hint)
	}

	msg.WriteByte(0x00) // null terminate the message

	msg.ResetLength(PGMessageLengthOffset)

	return msg.Bytes()
}

// ParseError parses a PG error message
func ParseError(e []byte) *Error {
	msg := NewMessageBuffer(e)
	msg.Seek(5)
	err := new(Error)

	for field, _ := msg.ReadByte(); field != 0; field, _ = msg.ReadByte() {
		value, _ := msg.ReadString()
		switch field {
		case ErrorFieldSeverity:
			err.Severity = value
		case ErrorFieldCode:
			err.Code = value
		case ErrorFieldMessage:
			err.Message = value
		case ErrorFieldMessageDetail:
			err.Detail = value
		case ErrorFieldMessageHint:
			err.Hint = value
		case ErrorFieldPosition:
			err.Position = value
		case ErrorFieldInternalPosition:
			err.InternalPosition = value
		case ErrorFieldInternalQuery:
			err.InternalQuery = value
		case ErrorFieldWhere:
			err.Where = value
		case ErrorFieldSchemaName:
			err.SchemaName = value
		case ErrorFieldTableName:
			err.TableName = value
		case ErrorFieldColumnName:
			err.ColumnName = value
		case ErrorFieldDataTypeName:
			err.DataTypeName = value
		case ErrorFieldConstraintName:
			err.Constraint = value
		case ErrorFieldFile:
			err.File = value
		case ErrorFieldLine:
			err.Line = value
		case ErrorFieldRoutine:
			err.Routine = value
		}
	}
	return err
}
