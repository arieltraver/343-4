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
var resetTimer *time.Timer
var mutex sync.Mutex // to lock global variables
// The RequestVote RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and majority votes


func (*RaftNode) RequestVote(arguments VoteArguments, reply *VoteReply) error {
	mutex.Lock()
	defer mutex.Unlock()

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
func randGen(floor int, ceiling int) int {
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	i := random.Intn(ceiling - floor)
	return i + floor
}


// The AppendEntry RPC as defined in Raft
// Hint 1: Use the description in Figure 2 of the paper
// Hint 2: Only focus on the details related to leader election and heartbeats
func (*RaftNode) AppendEntry(arguments AppendEntryArgument, reply *AppendEntryReply) error {
	mutex.Lock()
	defer mutex.Unlock()

	//reset the election timer
	t := randGen(50, 200)
	resetTimer = time.NewTimer(time.Millisecond * time.Duration(t))

	// if leader's term is less than current term, reject append entry request
	if arguments.Term < currentTerm {
		reply.Term = currentTerm
		reply.Success = false
		return nil
	}

	// if leader's term is greater or equal, update current term and set node as
	// follower
	currentTerm = arguments.Term
	isLeader = false
	reply.Term = currentTerm
	reply.Success = true
	
	return nil
}

// You may use this function to help with handling the election time out
// Hint: It may be helpful to call this method every time the node wants to start an election
func LeaderElection() {
	<- resetTimer.C //block until this timer is done
	//if it gets reset in the background, then it will continue waiting
	//possible data race, but this can't be locked here? probably not tho bc channel
	for {
		mutex.Lock()
		
		// initialize election
		voteCount := 1
		currentTerm++
		votedFor = selfID
		isLeader = false

		mutex.Unlock()

		arguments := VoteArguments{
			Term:        currentTerm,
			CandidateID: selfID,
		}

		// request votes from other nodes
		for _, server := range serverNodes {
			go func(server ServerConnection) {
				reply := VoteReply{}
				err := server.rpcConnection.Call("RaftNode.RequestVote", arguments, &reply)
				if err != nil {
					return
				}

				mutex.Lock()
				defer mutex.Unlock()

				if reply.Term > currentTerm { // reply contains a higher term
					currentTerm = reply.Term // update current term
					votedFor = -1 // reset votedFor
					isLeader = false // set node as follower
				} else if reply.ResultVote {
					voteCount++
					// majority of votes is reached
					if !isLeader && voteCount > len(serverNodes)/2 {
						isLeader = true // node is set as leader
						go Heartbeat() // start sending heartbeats
					}
				}
			}(server)
		}

		// sleep for random duration before starting a new election if needed
		// TODO: time.Timer instead
		time.Sleep(time.Duration(rand.Intn(150)+150) * time.Millisecond)
	}
}

// You may use this function to help with handling the periodic heartbeats
// Hint: Use this only if the node is a leader
func Heartbeat() {
	for {
		//TODO: use timer here
		mutex.Lock()
		// stop sending heartbeats if node stops being leader
		if !isLeader {
			mutex.Unlock()
			return
		}
		mutex.Unlock()

		arguments := AppendEntryArgument{
			Term: currentTerm,
			LeaderID: selfID,
		}
	
		for _, node := range(serverNodes) {
			go func(node ServerConnection) {
				reply := AppendEntryReply{}
				node.rpcConnection.Call("RaftNode.AppendEntry", arguments, &reply)
			}(node)
		}
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

	// Read the values sent in the command line

	// Get this sever's ID (same as its index for simplicity)
	myID, _ := strconv.Atoi(arguments[1])
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
	t := randGen(50, 200)
	resetTimer = time.NewTimer(time.Millisecond * time.Duration(t))
	// Once all the connections are established, we can start the typical operations within Raft
	// Leader election and heartbeats are concurrent and non-stop in Raft
	//idea 1: push a random number onto this, election timer grabs it from there and counts that much
	//idea 2: push a boolean onto this, election timer works if it's there, spawns new election if not
	//problem: if this is called at every response, will cause deadlock
	//solution: when pushing from update field, pop something off then put something new

	// HINT 1: You may need to start a thread here (or more, based on your logic)
	// Hint 2: Main process should never stop
	// Hint 3: After this point, the threads should take over
	// Heads up: they never will be done!
	// Hint 4: wg.Wait() might be helpful here

}
