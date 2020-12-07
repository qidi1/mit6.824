package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"../labgob"
	"../labrpc"
	"bytes"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//log entry
type Log struct {
	Index   int
	Term    int
	Content interface{}
}
type State int

const (
	FOLLOWER  State = 0
	CANDIDATE State = 1
	LEADER    State = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	//Persisten state on all servers
	currentTerm int   //当前的轮次
	voteFor     int   //投票给了谁
	logs        []Log //所有的log
	//Volatile state on all servers
	commitIndex int //具有的所有的log数量
	lastApplied int //已经提交的log数量
	//Volatile state on leaders
	nextIndex  []int //下一个需要发送的index
	matchIndex []int //与leader匹配的index序号
	//特殊属性
	heartTime      time.Timer    //心跳间隔
	selectTime     *time.Timer   //选举时间
	heartDuartion  time.Duration //心跳间隔时间
	selectDuartion time.Duration //选举间隔时间
	state          State         //当前状态
	voteNum        int           //投票给自己的人数
	//提交方法
	applyCh chan ApplyMsg
	//下一个applied的值
	nextApplied int
	//异步调用
	commitCond *sync.Cond
	//记录输入日志数量
	logsNum int
	killCh  chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.voteFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastApplied)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var voteFor int
	var logs []Log
	var lastApplied int
	var commitIndex int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil ||
		d.Decode(&lastApplied) != nil ||
		d.Decode(&commitIndex) != nil {
		fmt.Println("读取出错")
	} else {
		rf.currentTerm = currentTerm
		rf.voteFor = voteFor
		rf.logs = logs
		rf.lastApplied = lastApplied
		rf.commitIndex = commitIndex
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func Max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}

//变为follower，主要是状态变为follower，删除投给票的人
func (rf *Raft) TurnFollower(term int) {
	rf.ResetTime()
	rf.state = FOLLOWER
	rf.currentTerm = term
	rf.voteFor = -1
	rf.voteNum = 0
	fmt.Printf("%d成为follower,当前轮次%d\n", rf.me, rf.currentTerm)
}
func (rf *Raft) TurnLeader() {
	rf.ResetTime()
	fmt.Printf("%d成为leader,当前轮次%d\n", rf.me, rf.currentTerm)
	rf.state = LEADER
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			rf.nextIndex[i] = rf.commitIndex + 1
			rf.matchIndex[i] = rf.commitIndex
		} else {
			rf.nextIndex[i] = rf.commitIndex
			rf.matchIndex[i] = -1
		}
	}
	//todo 给别人发送自己当选的消息
	log := Log{
		Index: rf.commitIndex,
		Term:  rf.currentTerm,
	}
	rf.commitIndex++
	rf.logs = append(rf.logs, log)
	go rf.heartBeat()
}

//重置选举时间
func (rf *Raft) ResetTime() {
	//fmt.Printf("%d重置了选举时间,当前轮次%d\n", rf.me, rf.currentTerm)
	rf.selectTime.Stop()
	rf.selectTime = time.AfterFunc(rf.selectDuartion, func() {
		if rf.state != LEADER {
			//fmt.Println("进入选举阶段")
			rf.SendVoteRequest()
		}
	})

}

//尝试提交现在的applied值
func (rf *Raft) tryCommit() {
	//fmt.Printf("%d尝试提交，nextApplied为%d,lastApplied为%d\n", rf.me, rf.nextApplied, rf.lastApplied)
	rf.commitCond.Signal()
}

//等待条件
func (rf *Raft) waitCommitUntil(cond func() bool) {
	for !cond() {
		rf.commitCond.Wait()
	}
}

//提交
func (rf *Raft) commit() {
	for {
		if rf.killed() {
			return
		}
		var logs []Log
		logs = make([]Log, 0)
		rf.mu.Lock()
		/*
			rf.waitCommitUntil(func() bool {
				fmt.Printf("%d的nextApplied为%d,lastApplied为%d\n", rf.me, rf.nextApplied, rf.lastApplied)
				return rf.nextApplied > rf.lastApplied
			})
		*/
		for rf.nextApplied <= rf.lastApplied {
			if rf.killed() {
				return
			}
			rf.commitCond.Wait()
		}
		//为后面的压缩做准备，不使用index作为实际的下标
		var left int
		var right int
		left = rf.lastApplied - rf.logs[0].Index + 1
		right = rf.nextApplied - rf.logs[0].Index + 1
		fmt.Printf("%d进行了提交,LastApplied %d,nextApplied为%d\n", rf.me, rf.lastApplied, rf.nextApplied)
		logs = rf.logs[left:right]
		rf.lastApplied = rf.nextApplied
		rf.mu.Unlock()
		for _, log := range logs {
			reply := ApplyMsg{
				CommandValid: true,
				Command:      log.Content,
				CommandIndex: log.Index,
			}
			rf.applyCh <- reply
		}
	}

}

//判断现在应该提交的index
func (rf *Raft) leaderUpdateApplied() int {
	temp := make([]int, len(rf.peers))
	copy(temp, rf.matchIndex)
	majority := len(rf.peers) / 2
	sort.Ints(temp)
	return temp[majority]
}
func (rf *Raft) Majority() bool {

	return rf.voteNum > (len(rf.peers) / 2)
}

//比较申请者的log与自己的log
func (rf *Raft) LoglenCompare(args *RequestVoteArgs) bool {
	return (rf.commitIndex == 0 && args.LastLogTerm >= 0) ||
		rf.logs[rf.commitIndex-1].Term < args.LastLogTerm ||
		(rf.logs[rf.commitIndex-1].Term == args.LastLogTerm &&
			rf.commitIndex <= args.LastLogIndex)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	fmt.Printf("%d接收到选举请求，开始处理,请求者轮次%d,自身轮次%d\n", rf.me, args.Term, rf.currentTerm)
	if args.Term > rf.currentTerm {
		//如果新的一轮已经开始，则变为follower
		//rf.TurnFollower(args.Term)
		//在这里不能重置选举时间，log避免长度不够的server总是快一步
		rf.state = FOLLOWER
		rf.currentTerm = args.Term
		if rf.LoglenCompare(args) {
			rf.voteFor = args.CandidateId
			reply.Term = rf.currentTerm
			rf.ResetTime()
			fmt.Printf("%d向%d投票\n", rf.me, rf.voteFor)
			reply.VoteGranted = true
			return
		}
	} else if args.Term == rf.currentTerm {
		//如果轮次一样，而且没有进行投票，则进行投票
		if rf.LoglenCompare(args) && rf.voteFor == -1 {
			rf.voteFor = args.CandidateId
			fmt.Printf("%d向%d投票\n", rf.me, rf.voteFor)
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
			return
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	return
	// Your code here (2A, 2B).
}
func (rf *Raft) deviation(index int) int {
	return index - rf.logs[0].Index
}
func (rf *Raft) ReplyVote(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//如果自己不是candidate则丢弃消息
	if rf.state != CANDIDATE {
		return
	}
	//如果回复的轮次大于自己的轮次，便是已经开始新的一轮，将自己设为Follower
	if reply.Term > rf.currentTerm {
		rf.TurnFollower(reply.Term)
		return
	}
	//不是本轮投票的回复，直接丢弃
	if reply.Term < rf.currentTerm {
		return
	}
	//如果回复表示投票给自己，则将自己的的投票数加1
	if reply.VoteGranted {
		rf.voteNum++
		fmt.Printf("%d收到他人的投票，当前票数%d\n", rf.me, rf.voteNum)
		//如果收到的同意投票数大于一半，则将自己变为leader
		if rf.Majority() {
			rf.TurnLeader()
		}
	}
}

//发送选举请求，对于args和reply进行综合地处理
func (rf *Raft) SendVoteRequest() {
	rf.mu.Lock()
	rf.state = CANDIDATE
	rf.currentTerm++
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.commitIndex,
	}
	if rf.commitIndex > 0 {
		args.LastLogTerm = rf.logs[rf.commitIndex-1].Term
	} else {
		args.LastLogTerm = 0
	}
	rf.voteFor = rf.me
	rf.voteNum = 1
	rf.mu.Unlock()
	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}
		go func(index int) {
			//fmt.Printf("%d开始向%d发送选举请求\n", rf.me, index)
			reply := RequestVoteReply{}
			rf.sendRequestVote(index, &args, &reply)
			// fmt.Printf("%d开始处理%d选举回复\n", rf.me, index)
			rf.ReplyVote(reply)
		}(index)
	}
	rf.ResetTime()
}

//增加的Entry的参数
type AppendEntriesArgs struct {
	Term         int   //发布的轮次
	LeaderId     int   //leader的序号
	PreLogIndex  int   //发布的Log前一个log的index
	PreLogTerm   int   //发送的Log前一个log所处的轮次
	Entries      []Log //发送的所有的log Entries
	LeaderCommit int   //leader已经Commit的log的index
}

//对于增加Entry的回复
type AppendEntriesReply struct {
	Term    int  //回复消息所处于的轮次
	Success bool //这条消息是否被follower接受
}

//增加Log Entry
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	var success bool
	success = false
	//fmt.Printf("%d开始处理来自于leader%d的心跳\n", rf.me, args.LeaderId)
	if rf.currentTerm < args.Term {
		rf.TurnFollower(args.Term)
	} else if rf.currentTerm == args.Term {
		if rf.state == LEADER {
			//如果是轮次相等，且自身是leader产生bug
			fmt.Printf("在%d轮次产生了两个leader,%d,%d\n", args.Term, rf.me, args.LeaderId)
		} else if rf.state == CANDIDATE {
			//如果轮次相同，且自身是candidate将自己变为follower
			rf.TurnFollower(args.Term)
		} else {
			//如果轮次相同，且自身是follower，重置过期计时
			rf.ResetTime()
		}
	}
	if args.PreLogIndex < rf.commitIndex && args.PreLogIndex > -1 {
		//fmt.Printf("在%d当前log长度为%d,logs[PreLogIndex].Term为%d,发送过来的PreLogIndex为%d，PreLogTerm为%d\n", rf.me, rf.commitIndex, rf.logs[args.PreLogIndex].Term, args.PreLogIndex, args.PreLogTerm)
	} else {
		//fmt.Printf("在%d当前log长度为%d,PreLogIndex大于commitIndex,发送过来的PreLogIndex为%d，PreLogTerm为%d\n", rf.me, rf.commitIndex, args.PreLogIndex, args.PreLogTerm)
	}
	if rf.currentTerm == args.Term {
		//如果前一个log与之后的log的term相同表示之前的全部一样，对于轮次进行接收
		if args.PreLogIndex == -1 || rf.commitIndex == 0 ||
			(args.PreLogIndex < rf.commitIndex && rf.logs[args.PreLogIndex-rf.logs[0].Index].Term == args.PreLogTerm) {
			success = true
			if len(rf.logs) != 0 {
				rf.logs = rf.logs[0 : args.PreLogIndex-rf.logs[0].Index+1]
			}
			rf.logs = append(rf.logs, args.Entries...)
			rf.commitIndex = rf.logs[len(rf.logs)-1].Index + 1
			//to do完成对于log的写入对于为进行apply的log进行apply的操作
			rf.nextApplied = args.LeaderCommit
			//fmt.Printf("在%d接受了来自于leader%d增加log的请求,log长度为%d,LeaderCommit %d\n", rf.me, args.LeaderId, len(rf.logs), args.LeaderCommit)
			rf.tryCommit()
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = success
	return
}
func (rf *Raft) ReplyEntries(who int, reply *AppendEntriesReply, args *AppendEntriesArgs) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	//如果自身不为leader则对于回复消息进行丢弃
	if rf.state != LEADER {
		return
	}
	//如果回复的轮次大于自身的轮次，自身变为follower
	if reply.Term > rf.currentTerm {
		fmt.Printf("%d收到来自%d的心跳回复,已经落后于term,当前轮次%d\n", rf.me, who, rf.currentTerm)
		rf.TurnFollower(reply.Term)
		return
	}
	if reply.Term == -1 {
		//fmt.Printf("%d收到来自%d的超时的心跳回复,回复轮次%d,当前轮次%d\n", rf.me, who, reply.Term, rf.currentTerm)
		return
	}
	if reply.Term < rf.currentTerm {
		fmt.Printf("%d收到来自%d之前的心跳回复,回复轮次%d,当前轮次%d\n", rf.me, who, reply.Term, rf.currentTerm)
		return
	}
	if reply.Success {
		//如果回复成功，则对于现在follower拥有的最高的index，和下一个传送的index进行更新
		//fmt.Printf("对于%d的nextIndex未更改之前为%d,matchIndex为%d,nextApplied为%d\n", who, rf.nextIndex[who], rf.matchIndex[who], rf.nextApplied)
		rf.matchIndex[who] = Max(rf.matchIndex[who], args.PreLogIndex+len(args.Entries))
		rf.nextIndex[who] = Max(rf.nextIndex[who], args.PreLogIndex+len(args.Entries)+1)
		rf.nextApplied = rf.leaderUpdateApplied()
		//to do 对于超过半数的receive进行apply操作
		//fmt.Printf("对于%d的nextIndex更改为%d,matchIndex为%d,nextApplied为%d", who, rf.nextIndex[who], rf.matchIndex[who], rf.nextApplied)
		//fmt.Printf(",自身的nextIndex为%d，matchindex为%d\n", rf.nextIndex[rf.me], rf.matchIndex[rf.me])
		rf.tryCommit()
	} else {
		//如果不成功则对于nextIndex减一直到成功为止
		if rf.nextIndex[who] > 0 {
			rf.nextIndex[who] = rf.nextIndex[who] - 1
		}
	}
}

//发送Log增加请求，对于append和reply进行综合地处理
func (rf *Raft) SendAppendRequest() {
	for index := 0; index < len(rf.peers); index++ {
		if index == rf.me {
			continue
		}

		go func(index int) {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.state != LEADER {
				rf.mu.Unlock()
				return
			}
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.lastApplied,
				Entries:      rf.logs[rf.deviation(rf.nextIndex[index]):],
			}
			if rf.nextIndex[index] == 0 {
				args.PreLogIndex = -1
				args.PreLogTerm = 0
			} else {
				args.PreLogIndex = rf.nextIndex[index] - 1
				args.PreLogTerm = rf.logs[rf.deviation(rf.nextIndex[index])-1].Term
			}
			//fmt.Printf("%d当前log长度为%d，%d的nextIndex为%d，deviation为%d,len(entries)为%d\n", rf.me, rf.commitIndex, index, rf.nextIndex[index], rf.deviation(rf.nextIndex[index]), len(args.Entries))
			//fmt.Printf("%d向%d发送心跳\n", rf.me, index)
			reply := AppendEntriesReply{

				Term: -1,
			}
			rf.mu.Unlock()
			rf.sendRequestAppend(index, &args, &reply)
			rf.ReplyEntries(index, &reply, &args)
		}(index)
	}
	rf.ResetTime()
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//增加entry的rpc调用
func (rf *Raft) sendRequestAppend(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//定时函数，定时发送心跳
func (rf *Raft) heartBeat() {
	for {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != LEADER {
			break
		}
		rf.mu.Unlock()
		//fmt.Printf("%d开始发送心跳\n", rf.me)
		rf.SendAppendRequest()
		time.Sleep(rf.heartDuartion)
	}
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LEADER {
		isLeader = false
		// rf.mu.Unlock()
	} else {
		rf.logsNum++
		term = rf.currentTerm
		isLeader = true
		var newEntry = Log{
			Term:    term,
			Index:   rf.commitIndex,
			Content: command,
		}
		fmt.Printf("%d接收到%d号日志，内容为%v,进行发送\n", rf.me, rf.logsNum, command)
		rf.logs = append(rf.logs, newEntry)
		rf.nextIndex[rf.me] = rf.commitIndex + 1
		rf.matchIndex[rf.me] = rf.commitIndex
		index = rf.commitIndex
		rf.commitIndex++
		// rf.mu.Unlock()
		rf.SendAppendRequest()
		rf.persist()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.tryCommit()
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastApplied = -1
	rf.currentTerm = 0
	rf.nextApplied = -1
	rf.logs = make([]Log, 0)
	rf.applyCh = applyCh
	rf.dead = 0
	rf.voteFor = -1
	rf.heartDuartion = time.Millisecond * 40
	rf.selectDuartion = time.Duration((rand.Intn(150) + 150)) * time.Millisecond
	//调试参数
	//rf.heartDuartion = time.Second * 4
	//rf.selectDuartion = time.Duration((rand.Intn(4) + 12)) * time.Second
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.logsNum = 0
	rf.selectTime = time.AfterFunc(rf.selectDuartion, func() {
		if rf.killed() {
			return
		}
		if rf.state != LEADER {
			fmt.Printf("%d进入选举阶段\n", rf.me)
			rf.SendVoteRequest()
		}
	})
	/*
		log := Log{
			Index: 0,
			Term:  0,
		}
		rf.logs = append(rf.logs, log)
		rf.commitIndex++
	*/
	rf.commitCond = sync.NewCond(&rf.mu)
	go rf.commit()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
