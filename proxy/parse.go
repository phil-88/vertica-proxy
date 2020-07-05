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
	"regexp"
	"strconv"
	"strings"

	"github.com/crunchydata/crunchy-proxy/config"
	"github.com/crunchydata/crunchy-proxy/protocol"
)

// GetAnnotations the annotation approach
// assume a write if there is no comment in the SQL
// or if there are no keywords in the comment
// return (write, start, finish) booleans
func parseQuery(m []byte) map[AnnotationType]string {

	annotations := make(map[AnnotationType]string, 0)
	var query string

	message := protocol.NewMessageBuffer(m)
	messageType, _ := message.ReadByte()
	message.ReadInt32() // read message length
	if messageType == protocol.QueryMessageType {
		query, _ = message.ReadString()
	} else if messageType == protocol.ParseMessageType {
		message.ReadString()
		query, _ = message.ReadString()
	} else {
		return annotations
	}
	query = strings.ToLower(query)

	routes := config.GetQueryRoutes()
	for _, route := range routes {
		if match, _ := regexp.MatchString(route.Query, query); match {
			if annotations[RouteAnnotation] != config.GetDefaultBackend() {
				annotations[RouteAnnotation] = route.Target
			}
		}
	}

	if match, _ := regexp.MatchString("(\\s+|^)copy.*from\\s+vertica", query); match {
		annotations[NotSupportedAnnotation] = "copy from vertica"
	}

	if match, _ := regexp.MatchString("(\\s+|^)export\\s+to", query); match {
		annotations[NotSupportedAnnotation] = "export to vertica"
	}

	if match, _ := regexp.MatchString("(\\s+|^)connect\\s+", query); match {
		annotations[NotSupportedAnnotation] = "connect"
	}

	if match, _ := regexp.MatchString("(\\s+|^)disconnect\\s+", query); match {
		annotations[NotSupportedAnnotation] = "disconnect"
	}

	if match, _ := regexp.MatchString("(on\\s+commit|temp\\s+table|temporary\\s+table)", query); match {
		annotations[SessionLockAnnotation] = "temp table"
	}

	if match, _ := regexp.MatchString("set\\s+session", query); match &&
			!strings.Contains(query, "autocommit") && 
			!strings.Contains(query, "id") && 
			!strings.Contains(query, "detached") {
		annotations[SessionLockAnnotation] = "set session"
	}

	if match, _ := regexp.MatchString("(\\s+|^)(insert|delete|update)(\\s|\\/)", query); match {
		annotations[DMLAnnotation] = "dml"
	}

	if match, _ := regexp.MatchString("(\\s+|^)copy\\s+", query); match && !strings.Contains(query, "from vertica") {
		annotations[DMLAnnotation] = "copy"
	}

	if match, _ := regexp.MatchString("(\\s+|^)create\\s.*table\\s.*as\\s.*select(\\s|\\/)", query); match {
		annotations[DMLAnnotation] = "CTAS"
	}

	if match, _ := regexp.MatchString("select\\s+reset_session\\s*\\(\\s*\\)", query); match {
		annotations[SessionUnlockAnnotation] = "reset session"
	}

	if match, _ := regexp.MatchString("set\\s*(session)?\\s*autocommit\\s+to\\s+on", query); match {
		annotations[AutocommitAnnotation] = "on"
	}

	if match, _ := regexp.MatchString("set\\s*(session)?\\s*autocommit\\s+to\\s+off", query); match {
		annotations[AutocommitAnnotation] = "off"
	}

	if match, _ := regexp.MatchString("set\\s*(session)?\\s*id\\s+to\\s+'[^']*'", query); match {
		re := regexp.MustCompile("'[^']*'")
		sessionRaw := re.FindString(query)
		sessionID := sessionRaw[1 : len(sessionRaw)-1]
		if s, err := strconv.ParseInt(sessionID, 0, 32); err == nil {
			annotations[PickSession] = fmt.Sprint(s)
		}
	}

	if match, _ := regexp.MatchString("set\\s*(session)?\\s*detached\\s+to\\s+on", query); match {
		annotations[DetachSession] = "on"
	}

	if match, _ := regexp.MatchString("set\\s*(session)?\\s*detached\\s+to\\s+off", query); match {
		annotations[DetachSession] = "off"
	}


	//startPos := strings.Index(query, AnnotationStartToken)
	//endPos := strings.Index(query, AnnotationEndToken)

	//if startPos < 0 || endPos < 0 {
	//	return annotations
	//}

	//keywords := strings.Split(query[startPos+2:endPos], ",")

	//for i := 0; i < len(keywords); i++ {
	//	switch strings.TrimSpace(keywords[i]) {
	//	case readAnnotationString:
	//		annotations[ReadAnnotation] = true
	//	case startAnnotationString:
	//		annotations[StartAnnotation] = true
	//	case endAnnotationString:
	//		annotations[EndAnnotation] = true
	//	}
	//}

	return annotations
}
