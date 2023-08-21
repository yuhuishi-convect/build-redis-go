package main

import (
	"fmt"
	// Uncomment this block to pass the first stage
	"net"
	"os"
)

// handleRequest handles requests from clients.
func handleRequest(conn net.Conn) {
	defer conn.Close()
	// Make a buffer to hold incoming data.
	buf := make([]byte, 1024)
	// Read each message until newline.
	reqLen, err := conn.Read(buf)
	if err != nil {
		fmt.Println("Error reading: ", err.Error())
		return
	}

	// Print the incoming message.
	fmt.Println("Received ", string(buf[:reqLen]))

	// For each message, send a pong message back to the client.
	msg := "+PONG\r\n"
	_, err = conn.Write([]byte(msg))
	if err != nil {
		fmt.Println("Error writing to connection: ", err.Error())
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
