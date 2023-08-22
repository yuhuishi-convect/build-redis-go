package main

import (
	"fmt"
	"net"
	"os"
	"strings"
)

const (
	// redis command type
	redisCmdPing    = "PING"
	redisCmdEcho    = "ECHO"
	redisUnknownCmd = "UNKNOWN"
)

func parseRedisCommand(message []byte, messageLen int) (string, []string) {
	// parseRedisCommand parses the message and returns the command and arguments.
	// currently support PING and ECHO
	// PING example: *1\r\n$4\r\nPING\r\n
	// ECHO example: *2\r\n$4\r\nECHO\r\n$5\r\nHello\r\n

	// convert to string
	msg := string(message[:messageLen])

	// split by \r\n
	msgArr := strings.Split(msg, "\r\n")

	// get command
	cmd := strings.ToUpper(msgArr[2])

	// get arguments
	if cmd == redisCmdPing {
		return cmd, nil
	} else if cmd == redisCmdEcho {
		// ignore all length tokens
		args := msgArr[4:]
		return cmd, args

	} else {
		return redisUnknownCmd, nil
	}

}

// handleRequest handles requests from clients.
func handleRequest(conn net.Conn) {
	defer conn.Close()
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)

	for {
		// Read each message until newline.
		reqLen, err := conn.Read(buf)
		if err != nil {
			fmt.Println("Error reading: ", err.Error())
			break
		}

		// Print the incoming message.
		fmt.Println("Received ", string(buf[:reqLen]))

		// parse command
		cmd, args := parseRedisCommand(buf, reqLen)
		var msg string

		if cmd == redisCmdPing {
			msg = "+PONG\r\n"
		} else if cmd == redisCmdEcho {
			// join the echo message as a string
			msg = strings.Join(args, " ") + "\r\n"
			msg = "+" + msg
		} else {
			msg = "-ERR unknown command\r\n"
		}

		// For each message, send a pong message back to the client.
		_, err = conn.Write([]byte(msg))
		if err != nil {
			fmt.Println("Error writing to connection: ", err.Error())
		}
	}
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		fmt.Println("Error: ", err.Error())
		os.Exit(1)
	}

	// Use goroutines to handle multiple concurrent connections from clients.
	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// Handle the connection in a new goroutine.
		go handleRequest(conn)
	}
}
