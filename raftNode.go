package main

import (
	"bufio"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

// RaftNode represents a single Raft node in the cluster, providing the
// necessary methods to handle Raft RPC calls.
type RaftNode int

// VoteArguments contains the candidate information needed to request a vote
// from a Raft node during an election and for the node to decide whether to
// vote for the candidate in question.
type VoteArguments struct {
	Term        int
	CandidateID int
	Address     string
}

// VoteReply holds the response from a Raft node after processing a
// RequestVote RPC call.
type VoteReply struct {
	Term       int
	ResultVote bool
}

// AppendEntryArgument holds the information used by a leader to send an
// AppendEntry RPC call and for followers to determine whether the AppendEntry
// RPC call is valid.
type AppendEntryArgument struct {
	Term     int
	LeaderID int
	Address  string
}

// AppendEntryReply represents the response from a Raft node after processing a
// AppendEntry RPC call, including the follower's term and the success status
// of the call.
type AppendEntryReply struct {
	Term    int
	Success bool
}

type ServerConnection struct {
	serverID      int
	Address       string
	rpcConnection *rpc.Client
}

var selfID int
var serverNodes map[string]ServerConnection
var currentTerm int
var votedFor int
var isLeader bool
var myPort string
var mutex sync.Mutex // to lock global variables
var electionTimeout *time.Timer
var globalRand *rand.Rand

// resetElectionTimeout resets the election timeout to a new random duration.
// This function should be called whenever an event occurs that prevents the need for a new election,
// such as receiving a heartbeat from the leader or granting a vote to a candidate.
func resetElectionTimeout() {
	duration := time.Duration(globalRand.Intn(150)+151) * time.Millisecond
	electionTimeout.Stop()          // Use Reset method only on stopped or expired timers
	electionTimeout.Reset(duration) // Resets the timer to new random value
}

// RequestVote processes a RequestVote RPC call from a candidate and decides
// whether to vote for the candidate based on whether it has not voted in this
// term and the value of the candidate's term.
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// Reject vote request if candidate's term is lower than current term
	if arguments.Term < currentTerm {
		//fmt.Println(arguments.CandidateID, "has term:", arguments.Term, "but current term is", currentTerm)

		// If candidate has lower term, it may have failed and come back. Call
		// Reconnect() to try to update its rpc.Connection value in serverNodes
		err := Reconnect(arguments.CandidateID, arguments.Address)
		if err != nil {
			log.Println("Error in reconnect:", err)
		}

		reply.Term = currentTerm
		reply.ResultVote = false
		return nil
	}

	if arguments.Term > currentTerm {
		currentTerm = arguments.Term // update current term
		votedFor = -1                // has not voted in this new term
	}

	reply.Term = currentTerm
	//fmt.Println(currentTerm)

	// Grant vote if node has not voted
	if votedFor == -1 {
		//fmt.Println("voting for candidate", arguments.CandidateID)
		reply.ResultVote = true
		votedFor = arguments.CandidateID
		resetElectionTimeout()
	} else {
		reply.ResultVote = false
	}
	return nil
}

func Reconnect(newId int, address string) error {
	client, err := rpc.DialHTTP("tcp", address)
	for err != nil {
		// Record it in log
		log.Println("Trying again. Connection error: ", err)
		// Try again!
		client, err = rpc.DialHTTP("tcp", address)
	}
	fmt.Println("Reconnected with", address)

	// close old connection and replace with new connection
	serverNodes[address].rpcConnection.Close()
	serverNodes[address] = ServerConnection{serverID: newId, Address: address, rpcConnection: client}

	return nil
}

/*
sleeps and updates the timer, unless electionHappened
//if electionhappened, resets the timer.
//
//if timer hits zero, it calls new election, which will return to this OR go to leader.
//
//this is the default "main" state of a non-leader node
*/
// The AppendEntry RPC as defined in Raft
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	// if leader's term is less than current term, reject append entry request
	if arguments.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		isLeader = false
		Reconnect(arguments.LeaderID, arguments.Address)
		return nil
	}

	// if leader's term is greater or equal, update current term and set node as
	// follower
	currentTerm = arguments.Term
	isLeader = false
	reply.Term = currentTerm
	reply.Success = true
	resetElectionTimeout() // heartbeat indicates a leader, so no new election
	fmt.Println("received heartbeat.")

	return nil
}

func LeaderElection() {
	for {
		<-electionTimeout.C // wait for election timeout
		mutex.Lock()
		// check if node is already leader so loop does not continue
		if isLeader {
			fmt.Println("ending leaderelection because I am now leader")
			mutex.Unlock()
			return
		}

		// --- initialize election
		currentTerm++ // new term
		votedFor = selfID

		mutex.Unlock()

		voteCount := 1 // votes for itself

		arguments := VoteArguments{
			Term:        currentTerm,
			CandidateID: selfID,
			Address:     myPort,
		}

		// request votes from other nodes
		//fmt.Println("Requesting votes")
		for _, server := range serverNodes {
			go func(server ServerConnection) {
				reply := VoteReply{}
				err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
				if err != nil {
					return
				}

				mutex.Lock()
				defer mutex.Unlock()

				if reply.Term > currentTerm {
					currentTerm = reply.Term // update current term
					votedFor = -1            // reset votedFor
				} else if reply.ResultVote {
					voteCount++
					// receives votes from a majority of the servers
					if !isLeader && voteCount > len(serverNodes)/2 {
						//fmt.Println("leader! ->", voteCount, "votes for", selfID)
						isLeader = true // enters leader state
						go Heartbeat()  // begins sending heartbeats
					}
				}

			}(server)
		}
		resetElectionTimeout()
	}
}

// Heartbeat is used when the current node is a leader and handles the periodic
// sending of heartbeat messages to other nodes in the cluster to establish its
// role as leader.
func Heartbeat() {
	heartbeatTimer := time.NewTimer(100 * time.Millisecond)
	for {
		<-heartbeatTimer.C
		mutex.Lock()
		if !isLeader {
			mutex.Unlock()
			return
		}
		mutex.Unlock()

		arguments := AppendEntryArgument{
			Term:     currentTerm,
			LeaderID: selfID,
			Address:  myPort,
		}

		//fmt.Println("Sending heartbeats")
		for _, server := range serverNodes {
			go func(server ServerConnection) {
				reply := AppendEntryReply{}
				server.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
			}(server)
		}
		heartbeatTimer.Reset(100 * time.Millisecond)
	}
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// --- Read the values sent in the command line

	// Get this server's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])

	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort = "localhost"

	// --- Read the IP:port info from the cluster configuration file
	scanner := bufio.NewScanner(file)
	lines := make([]string, 0)
	index := 0
	for scanner.Scan() {
		// Get server IP:port
		text := scanner.Text()
		log.Printf(text, index)
		if index == myID {
			myPort = text
			index++
			continue
		}
		// Save that information as a string for now
		lines = append(lines, text)
		index++
	}
	// If anything wrong happens with reading the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// -- Initialize global variables
	selfID = myID
	currentTerm = 0
	votedFor = -1
	isLeader = false // starts in the follower state
	mutex = sync.Mutex{}

	globalRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	tRandom := time.Duration(globalRand.Intn(150)+150) * time.Millisecond
	electionTimeout = time.NewTimer(tRandom)

	// --- Register the RPCs of this object of type RaftNode
	api := new(RaftNode)
	err = rpc.Register(api)
	if err != nil {
		log.Fatal("error registering the RPCs", err)
	}
	rpc.HandleHTTP()
	go http.ListenAndServe(myPort, nil)
	log.Printf("serving rpc on port" + myPort)

	// This is a workaround to slow things down until all servers are up and running
	// Idea: wait for user input to indicate that all servers are ready for connections
	// Pros: Guaranteed that all other servers are already alive
	// Cons: Non-realistic work around

	// reader := bufio.NewReader(os.Stdin)
	// fmt.Print("Type anything when ready to connect >> ")
	// text, _ := reader.ReadString('\n')
	// fmt.Println(text)

	// Idea 2: keep trying to connect to other servers even if failure is encountered
	// For fault tolerance, each node will continuously try to connect to other nodes
	// This loop will stop when all servers are connected
	// Pro: Realistic setup
	// Con: If one server is not set up correctly, the rest of the system will halt

	serverNodes = make(map[string]ServerConnection)
	for index, element := range lines {
		// Attempt to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			//log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		// serverNodes = append(serverNodes, ServerConnection{index, element, client})
		serverNodes[element] = ServerConnection{index, element, client}
		// Record that in log
		fmt.Println("Connected to " + element)
	}

	fmt.Println("My ID", selfID, myID)
	for i, node := range serverNodes {
		fmt.Println(i, "| ID | ", node.serverID, "| address |", node.Address)
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go LeaderElection() // concurrent and non-stop leader election
	wg.Wait()           // waits forever, so main process does not stop

	// Once all the connections are established, we can start the typical operations within Raft
	//idea 1: push a random number onto this, election timer grabs it from there and counts that much
	//idea 2: push a boolean onto this, election timer works if it's there, spawns new election if not
	//problem: if this is called at every response, will cause deadlock
	//solution: when pushing from update field, pop something off then put something new

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 3: After this point, the threads should take over
}
