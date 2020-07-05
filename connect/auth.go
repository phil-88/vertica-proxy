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
	"bytes"
	"crypto/md5"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"math/rand"

	"github.com/crunchydata/crunchy-proxy/config"
	"github.com/crunchydata/crunchy-proxy/protocol"
	"github.com/crunchydata/crunchy-proxy/util/log"
)

/*
 * Handle authentication requests that are sent by the backend to the client.
 *
 * connection - the connection to authenticate against.
 * message - the authentication message sent by the backend.
 */
func HandleAuthenticationRequest(connection net.Conn, message []byte, role string) bool {
	var msgType byte
	var msgLength int32
	var authType int32

	// Read message type.
	msgType = message[0]

	// Read message length.
	reader := bytes.NewReader(message[1:5])
	binary.Read(reader, binary.BigEndian, &msgLength)

	// Read authentication type.
	reader.Reset(message[5:9])
	binary.Read(reader, binary.BigEndian, &authType)

	if msgType == protocol.ErrorMessageType {
		log.Errorf("Authentication failed: %s", message[5:msgLength-4])
		return false
	}

	switch authType {
	case protocol.AuthenticationKerberosV5:
		log.Error("KerberosV5 authentication is not currently supported.")
	case protocol.AuthenticationClearText:
		log.Info("Authenticating with clear text password.")
		return handleAuthClearText(connection, role)
	case protocol.AuthenticationMD5:
		log.Info("Authenticating with MD5 password.")
		return handleAuthMD5(connection, message, role)
	case protocol.AuthenticationSCM:
		log.Error("SCM authentication is not currently supported.")
	case protocol.AuthenticationGSS:
		log.Error("GSS authentication is not currently supported.")
	case protocol.AuthenticationGSSContinue:
		log.Error("GSS authentication is not currently supported.")
	case protocol.AuthenticationSSPI:
		log.Error("SSPI authentication is not currently supported.")
	case protocol.AuthenticationOk:
		/* Covers the case where the authentication type is 'cert' or 'trust' */
		return true
	default:
		log.Errorf("Unknown authentication method: %d", authType)
	}

	return false
}

func createMD5Password(username string, password string, salt string) string {
	// Concatenate the password and the username together.
	passwordString := fmt.Sprintf("%s%s", password, username)

	// Compute the MD5 sum of the password+username string.
	passwordString = fmt.Sprintf("%x", md5.Sum([]byte(passwordString)))

	// Compute the MD5 sum of the password hash and the salt
	passwordString = fmt.Sprintf("%s%s", passwordString, salt)
	return fmt.Sprintf("md5%x", md5.Sum([]byte(passwordString)))
}

func handleAuthMD5(connection net.Conn, message []byte, role string) (bool) {
	// Get the authentication credentials.
	creds := config.GetCredentials(role)
	username := creds.Username
	password := creds.Password
	salt := string(message[9:13])

	password = createMD5Password(username, password, salt)

	// Create the password message.
	passwordMessage := protocol.CreatePasswordMessage(password)

	// Send the password message to the backend.
	_, err := Send(connection, passwordMessage)

	// Check that write was successful.
	if err != nil {
		log.Error("Error sending password message to the backend.")
		log.Errorf("Error: %s", err.Error())
	}

	// Read response from password message.
	var response []byte
	response, _, err = RecieveSingleMessage(connection)

	// Check that read was successful.
	if err != nil {
		log.Error("Error receiving authentication response from the backend.")
		log.Errorf("Error: %s", err.Error())
	}

	return protocol.IsAuthenticationOk(response)
}

func handleAuthClearText(connection net.Conn, role string) bool {
	creds := config.GetCredentials(role)
	password := creds.Password
	passwordMessage := protocol.CreatePasswordMessage(password)

	_, err := Send(connection, passwordMessage)

	if err != nil {
		log.Error("Error sending clear text password message to the backend.")
		log.Errorf("Error: %s", err.Error())
	}

	// response := make([]byte, 4096)
	// _, err = connection.Read(response)
	var response []byte
	response, _, err = RecieveSingleMessage(connection)

	if err != nil {
		log.Error("Error receiving clear text authentication response.")
		log.Errorf("Error: %s", err.Error())
	}

	return protocol.IsAuthenticationOk(response)
}

// AuthenticateClient - Establish and authenticate client connection to the backend.
//
//  This function simply handles the passing of messages from the client to the
//  backend necessary for startup/authentication of a connection. All
//  communication is between the client and the master node. If the client
//  authenticates successfully with the master node, then 'true' is returned and
//  the authenticating connection is terminated.
func AuthenticateClient(client net.Conn, master net.Conn, message []byte, length int, 
						clientName string, proceed bool) (bool, error, int32, int32, int32) {
	var err error
	authMethod := int32(-1)
	backendPid := int32(0)
	backendSecret := int32(0)

	/* Relay the startup message to master node. */
	log.Debugf("%s: client auth: relay startup message to 'master' node (%s)", clientName, master.RemoteAddr().String())
	_, err = Send(master, message[:length])

	/* Receive startup response. */
	log.Debugf("%s: client auth: receiving startup response from 'master' node", clientName)
	message, length, err = RecievePendingMessages(master)
	log.Debugf("%s: client auth: master: %s", clientName, log.GetReadableDatagram(message[:length], length))

	if err != nil {
		log.Errorf("%s: An error occurred receiving startup response.", clientName)
		log.Errorf("%s: Error: %s", clientName, err.Error())
		return false, err, authMethod, backendPid, backendSecret
	}

	/*
	 * While the response for the master node is not an AuthenticationOK or
	 * ErrorResponse keep relaying the mesages to/from the client/master.
	 */
	messageType := protocol.GetMessageType(message)
	authMethod = protocol.GetAuthenticationMethod(message)

	for !protocol.IsAuthenticationOk(message) && (messageType != protocol.ErrorMessageType) {

		Send(client, message[:length])
		message, length, err = RecievePendingMessages(client)

		logMessage := log.GetReadableDatagram(message[:length], length)
		if (length > 0 && protocol.GetMessageType(message[:length]) == protocol.PasswordMessageType) {
			logMessage = "password goes here"
		}
		log.Debugf("%s: client auth: client: %s", clientName, logMessage)

		/*
		 * Must check that the client has not closed the connection.  This in
		 * particular is specific to 'psql' when it prompts for a password.
		 * Apparently, when psql prompts the user for a password it closes the
		 * original connection, and then creates a new one. Eventually the
		 * following send/receives would timeout and no 'meaningful' messages
		 * are relayed. This would ultimately cause an infinite loop.  Thus it
		 * is better to short circuit here if the client connection has been
		 * closed.
		 */
		if (err != nil) && (err == io.EOF) {
			log.Infof("%s: The client closed the connection.", clientName)
			log.Debugf("%s: If the client is 'psql' and the authentication method " +
				"was 'password', then this behavior is expected.", clientName)
			return false, err, authMethod, backendPid, backendSecret
		}

		Send(master, message[:length])

		message, length, err = RecieveSingleMessage(master)
		messageType = protocol.GetMessageType(message)
		log.Debugf("%s: client auth: master: %s", clientName, log.GetReadableDatagram(message[:length], length))
	}

	/*
	 * If the last response from the master node was AuthenticationOK, then
	 * terminate the connection and return 'true' for a successful
	 * authentication of the client.
	 */
	log.Debugf("%s: client auth: checking authentication repsonse", clientName)
	if protocol.IsAuthenticationOk(message) {

		Send(client, message[:length])

		for messageType != protocol.ReadyForQueryMessageType && messageType != protocol.ErrorMessageType {

			message, length, err = RecieveSingleMessage(master)
			messageType = protocol.GetMessageType(message[:length])
			
			log.Debugf("%s: client auth: master: %s", clientName, log.GetReadableDatagram(message[:length], length))

			if proceed || messageType != protocol.ReadyForQueryMessageType {
				Send(client, message[:length])
			}

			if messageType == protocol.BackendKeyDataMessageType {
				backendPid = protocol.GetBackendPid(message[:length])
				backendSecret = protocol.GetBackendSecret(message[:length])
			}
		}

		return true, nil, authMethod, backendPid, backendSecret
		
	} else if protocol.GetMessageType(message) == protocol.ErrorMessageType {

		err = protocol.ParseError(message)
		log.Errorf("%s: Error occurred on client startup.", clientName)
		log.Errorf("%s: Error: %s", clientName, err.Error())

		Send(client, message[:length])

		return false, err, authMethod, backendPid, backendSecret
	} 

	log.Errorf("%s: Unkown error during authorization", clientName)

	return false, err, authMethod, backendPid, backendSecret
}

func AuthenticateBackend(connection net.Conn, role string) (bool,int32,int32) {
	backendPid := int32(0)
	backendSecret := int32(0)

	cred := config.GetCredentials(role)
	username := cred.Username //config.GetStringMapString("credentials.username")
	database := cred.Database //config.GetStringMapString("credentials.database")
	options := cred.Options //config.GetStringMapString("credentials.options")
	protocolVersion := protocol.ParseProtocolVersionString(cred.Protocol)

	startupMessage := protocol.CreateStartupMessage(username, database, options, protocolVersion)

	Send(connection, startupMessage)

	response, _, err := RecieveSingleMessage(connection)
	if err != nil {
		return false, backendPid, backendSecret
	}

	authenticated := HandleAuthenticationRequest(connection, response, role)

	if !authenticated {
		return false, backendPid, backendSecret
	} else {

		messageType := byte(0)

		var message []byte
		var err error

		for messageType != protocol.ReadyForQueryMessageType &&
				messageType != protocol.ErrorMessageType &&
				messageType != protocol.TerminateMessageType {

			message, _, err = RecieveSingleMessage(connection)
			if err != nil {
				return false, backendPid, backendSecret
			}

			messageType = protocol.GetMessageType(message)

			if messageType == protocol.BackendKeyDataMessageType {
				backendPid = protocol.GetBackendPid(message)
				backendSecret = protocol.GetBackendSecret(message)
			}
		} 
		return true, backendPid, backendSecret
	}
}

func FakeAuthenticateClient(client net.Conn, message []byte, length int, clientName string) (bool, error, int32, int32) {
	var err error
	backendPid := int32(rand.Intn(0x7fffffff))
	backendSecret := int32(rand.Intn(0x7fffffff))

	authMsg := protocol.GetAuthenticationOkMessage()
	backendMsg := protocol.GetBackendKeyDataMessage(backendPid, backendSecret)
	encodingMsg := protocol.GetParameterStatusMessage("client_encoding", "UNICODE")
	readyMsg := protocol.GetReadyForQueryMessage()

	Send(client, authMsg)
	Send(client, backendMsg)
	Send(client, encodingMsg)
	Send(client, readyMsg)

	return true, err, backendPid, backendSecret
}