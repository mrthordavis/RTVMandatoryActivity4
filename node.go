package main

import (
	pb "RTVMandatoryActivity4/gRPC"
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Server struct {
	pb.UnimplementedRicartAgrawalaServer
	id               string
	state            int32 //0: Released, 1: Wanted, 2: Held
	lamport          int32
	requestTimestamp int32
	knownNodesMap    map[string]string
	clients          map[string]pb.RicartAgrawalaClient
	release          chan struct{}
	mu               sync.Mutex
}

func (s *Server) sendTick() int32 {
	s.mu.Lock()
	s.lamport++
	returnValue := s.lamport
	log.Println("[TICK] Lamport ticked to: " + strconv.Itoa(int(s.lamport)))
	s.mu.Unlock()

	return returnValue
}

func (s *Server) sendTickLocked() int32 {
	s.lamport++
	returnValue := s.lamport
	log.Println("[TICK] Lamport ticked to: " + strconv.Itoa(int(s.lamport)))
	return returnValue
}

func (s *Server) mergeAndTick(externalTime int32) int32 {
	s.mu.Lock()
	log.Println("[MERGE] Merging between local timestamp: " + strconv.Itoa(int(s.lamport)) + " and message timestamp: " + strconv.Itoa(int(externalTime)) + ", and ticking by one.")
	s.lamport = max(externalTime, s.lamport) + 1
	returnValue := s.lamport
	log.Println("[TICK] Lamport merged and ticked to: " + strconv.Itoa(int(s.lamport)))
	s.mu.Unlock()

	return returnValue
}

func main() {
	// LOGGING
	os.MkdirAll("logs", 0755)

	var myID string = os.Args[1]
	var myPort string
	var logPath string = "logs/" + myID + ".log"

	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal(err)
	}

	writer := io.MultiWriter(os.Stdout, logFile)
	log.SetOutput(writer)
	log.SetPrefix("[SERVER " + myID + "] ")

	defer logFile.Close()

	server := Server{
		id:               myID,
		lamport:          0,
		knownNodesMap:    make(map[string]string),
		clients:          make(map[string]pb.RicartAgrawalaClient),
		release:          make(chan struct{}),
		state:            0,
		requestTimestamp: 0,
	}

	// READING FROM ID MAP FILE
	idFile, err := os.Open("idMap.txt")
	if err != nil {
		log.Fatal(err)
	}

	defer idFile.Close()

	// Set own ID and fill address map
	reader := bufio.NewScanner(idFile)
	for reader.Scan() {
		parts := strings.Fields(reader.Text())
		if parts[0] == myID {
			myPort = parts[1]
		} else {
			server.knownNodesMap[parts[0]] = "localhost:" + parts[1]
		}
	}

	// Listen and serve own server
	listener, _ := net.Listen("tcp", "localhost:"+myPort)

	grpcServer := grpc.NewServer()

	pb.RegisterRicartAgrawalaServer(grpcServer, &server)

	log.Println("[STARTUP] Server has started at logical time: 0")

	go grpcServer.Serve(listener)

	//Fill clients map with new RicartAgrawalaClients based on the knownNodesMap
	for k, v := range server.knownNodesMap {
		conn, err := grpc.NewClient(v, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if (err) != nil {
			log.Fatal(err)
		}
		defer conn.Close()
		server.clients[k] = pb.NewRicartAgrawalaClient(conn)
	}

	scanner := bufio.NewScanner(os.Stdin)

	// Manually make this node request by writing "request" in the terminal window
	for scanner.Scan() {
		if strings.ToLower(scanner.Text()) == "request" {
			go server.requestAccessToCS()
		}
	}

	grpcServer.Stop()

	server.mu.Lock()
	log.Println("[SHUTDOWN] Server has shutdown at logical time: " + strconv.Itoa(int(server.lamport)))
	server.mu.Unlock()
}

// Method for requesting all other nodes, and then entering the critcal section when all replies have arrived.
func (s *Server) requestAccessToCS() {
	s.mu.Lock()
	if s.state == 0 {
		// Set state to wanted
		s.state = 1

		// Set the timestamp for this node's latest request.
		s.requestTimestamp = s.sendTickLocked()
		localTimeStamp := s.requestTimestamp
		s.mu.Unlock()

		var wg sync.WaitGroup

		// Send out requests to all other nodes, and wait for their replies with a waitgroup.
		for id, rac := range s.clients {
			wg.Add(1)
			go s.singleRequest(&wg, id, rac, localTimeStamp)
		}
		wg.Wait()

		// Set state to held, do work in the critical section, and release it when done.
		s.mu.Lock()
		s.state = 2
		lamportForCS := s.lamport
		s.mu.Unlock()
		s.criticalSection(int(lamportForCS))
		s.releaseCriticalSection()
	} else {
		// Ignore and unlock if already requesting or holding. Should only happen if user manually writes "request" in the terminal while already requesting or holding.
		s.mu.Unlock()
	}
}

// Method for releasing the critical section, locks locally.
func (s *Server) releaseCriticalSection() {
	s.mu.Lock()
	if s.state == 2 {
		s.state = 0
		close(s.release)
		s.release = make(chan struct{})
	}
	s.mu.Unlock()
}

// Method for sending a single request, and waiting for its reply
func (s *Server) singleRequest(wg *sync.WaitGroup, peerID string, client pb.RicartAgrawalaClient, timestamp int32) {
	defer wg.Done()

	reply, err := client.Request(context.Background(),
		&pb.RequestMsg{Id: s.id, Lamport: timestamp},
		grpc.WaitForReady(true))
	if err != nil {
		log.Fatal(err)
	}
	s.mergeAndTick(reply.Lamport)
}

// Request handler for handling other nodes' requests
func (s *Server) Request(ctx context.Context, req *pb.RequestMsg) (*pb.ReplyMsg, error) {
	s.mergeAndTick(req.Lamport)
	s.mu.Lock()

	// Check whether i have priority, or if i should reply immediately
	var mustDefer bool = s.state == 2 || (s.state == 1 && (s.requestTimestamp < req.Lamport || (s.requestTimestamp == req.Lamport && s.id < req.Id)))
	release := s.release
	s.mu.Unlock()

	if mustDefer {
		//Defer request, i have priority
		<-release
	}

	msg := &pb.ReplyMsg{Id: s.id, Lamport: s.sendTick()}
	return msg, nil
}

// Critical section go routine w. print statement and sleep
func (s *Server) criticalSection(lamport int) {
	log.Println(s.id + " is entering the critical section at time: " + strconv.Itoa(lamport))

	log.Println(s.id + " is doing work in the critical section and is 'working' for 10 seconds")

	time.Sleep(10 * time.Second)

	log.Println(s.id + " is releasing the critical section at time: " + strconv.Itoa(lamport))
}
