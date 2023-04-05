package main

import (
	"bufio"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"math/rand"
	"time"
)

type RaftNode int

type VoteArguments struct {
	Term         int
	CandidateID  int
}

type VoteReply struct {
	Term       int
	ResultVote bool
}

type AppendEntryArgument struct {
	Term         int
	LeaderID     int
}

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
var serverNodes []ServerConnection
var currentTerm int
var votedFor int
var isLeader bool
var mutex sync.Mutex

// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes
func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	//reply.Term = r.currentTerm
	//reply.ResultVote = false

	if arguments.Term < currentTerm { // 
		reply.Term = currentTerm // candidate's term less than current term
		reply.ResultVote = false
		return nil
	}

	if arguments.Term > currentTerm {
		currentTerm = arguments.Term // update current term
		votedFor = -1
	}

	if votedFor == -1 || votedFor == arguments.CandidateID {
		reply.Term = currentTerm
		reply.ResultVote = true
		votedFor = arguments.CandidateID
	} else {
		reply.Term = currentTerm
		reply.ResultVote = false
	}

	return nil
}

//generates a random integer between floor and ceiling half-open. do not input negative ceiling
func (*RaftNode) randGen(floor int, ceiling int) int {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	i := random.Intn(ceiling - floor)
	return i + floor
}

/*sleeps and updates the timer, unless electionHappened
//if electionhappened, resets the timer.
//
//if timer hits zero, it calls new election, which will return to this OR go to leader.
//
//this is the default "main" state of a non-leader node
*/
func (r *RaftNode) electionTimer(electionHappened chan bool ) {
	timer := r.randGen(50, 100) //TODO: change these numbers
	timerIsZero := make(chan bool, 1)
	for {
		select {
		case <- electionHappened: //to be filled by vote function
			timer = r.randGen(1, 50)
		case <- timerIsZero:
			LeaderElection()
		default:
			time.Sleep(1)
			timer--
			if timer == 0 {
				timerIsZero <- true
			}
		}
	}
}

// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	if arguments.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return nil
	}

	currentTerm = arguments.Term
	isLeader = false
	reply.Term = currentTerm
	reply.Success = true
	
	return nil
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	for {
		mutex.Lock()
		
		voteCount := 1
		
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat(r *RaftNode) {
	timer := r.randGen(10, 40) //TODO: change these numbers
	timerIsZero := make(chan bool, 1)
	for {
		select {
		case <- timerIsZero:
			r.SendHeartBeat()
			timer = r.randGen(10, 40)
		default:
			time.Sleep(1)
			timer--
			if timer == 0 {
				timerIsZero <- true
			}
		}
	}
}

func (r *RaftNode) SendHeartBeat() {
	//send a heartbeat to all nodes
	//for node in []serverNodes...
}

func main() {
	// The assumption here is that the command line arguments will contain:
	// This server's ID (zero-based), location and name of the cluster configuration file
	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide cluster information.")
		return
	}

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, err := strconv.Atoi(arguments[1])
	// Get the information of the cluster configuration file containing information on other servers
	file, err := os.Open(arguments[2])
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	myPort := "localhost"

	// Read the IP:port info from the cluster configuration file
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
	// If anything wrong happens with readin the file, simply exit
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	// Following lines are to register the RPCs of this object of type RaftNode
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

	for index, element := range lines {
		// Attemp to connect to the other server node
		client, err := rpc.DialHTTP("tcp", element)
		// If connection is not established
		for err != nil {
			// Record it in log
			log.Println("Trying again. Connection error: ", err)
			// Try again!
			client, err = rpc.DialHTTP("tcp", element)
		}
		// Once connection is finally established
		// Save that connection information in the servers list
		serverNodes = append(serverNodes, ServerConnection{index, element, client})

		// Record that in log
		fmt.Println("Connected to " + element)
	}

	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
