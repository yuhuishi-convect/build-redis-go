package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	// redis command type
	redisCmdPing    = "PING"
	redisCmdEcho    = "ECHO"
	redisCmdSet     = "SET"
	redisCmdGet     = "GET"
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
	} else if cmd == redisCmdSet {
		keyVal := msgArr[4]
		valueVal := msgArr[6]
		args := []string{keyVal, valueVal}
		// checks if PX is specified
		numArgs := len(msgArr)
		if numArgs > 8 {
			// PX is specified
			expireInMS := msgArr[10]
			args = append(args, expireInMS)
		} else {
			args = append(args, "")
		}
		return cmd, args

	} else if cmd == redisCmdGet {
		keyVal := msgArr[4]
		args := []string{keyVal}
		return cmd, args

	} else {
		return redisUnknownCmd, nil
	}

}

type RedisVal struct {
	value     interface{}
	expiredAt *int64 // unix timestamp
}

var db = make(map[interface{}]RedisVal)

func handleSetCommand(key interface{}, value interface{}, expireInMS *int64) {

	// handleSetCommand handles SET command.
	redisVal := RedisVal{
		value:     value,
		expiredAt: nil,
	}

	// set expire time if expireInMS is not nil
	if expireInMS != nil {
		// get current unix timestamp in milliseconds
		now := time.Now().Unix() * 1000
		// set expiredAt
		expiredAt := now + *expireInMS
		redisVal.expiredAt = &expiredAt
	}

	// save to db
	db[key] = redisVal
}

func handleGetCommand(key interface{}) (interface{}, bool) {
	// handleGetCommand handles GET command.
	// get from db
	value, ok := db[key]
	if !ok {
		return nil, false
	}

	// check if expired
	if value.expiredAt != nil {
		// get current unix timestamp in milliseconds
		now := time.Now().Unix() * 1000
		if now > *value.expiredAt {
			// expired
			fmt.Println("Expired at", *value.expiredAt, "Now", now)
			return nil, false
		}
	}

	return value.value, true

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
			msg = strings.Join(args, "") + "\r\n"
			msg = "+" + msg
		} else if cmd == redisCmdSet {
			keyVal := args[0]
			valueVal := args[1]
			expireInMS := args[2]
			// convert expireInMS to int if not empty
			var expireInMSInt *int64
			if expireInMS != "" {
				expireInMSIntVal, err := strconv.ParseInt(expireInMS, 10, 64)
				if err != nil {
					fmt.Println("Error parsing expireInMS", err.Error())
					os.Exit(1)
				}
				expireInMSInt = &expireInMSIntVal

			} else {
				expireInMSInt = nil
			}
			handleSetCommand(keyVal, valueVal, expireInMSInt)
			msg = "+OK\r\n"
		} else if cmd == redisCmdGet {
			keyVal := args[0]
			value, ok := handleGetCommand(keyVal)
			if ok {
				// return as a simple string
				msg = "+" + value.(string) + "\r\n"
			} else {
				// return a nil bulk string
				msg = "$-1\r\n"
			}

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
