package raft

import (
	"bytes"
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"

	"wang.deng/raft-kv/labgob"
	"wang.deng/raft-kv/rpcutil"
)
import "sync/atomic"

// 用于将日志命令Apply到状态机的数据结构
type ApplyMsg struct {
	CommandValid bool	// 命令是否有效
	Command      interface{}	// 具体的命令
	CommandIndex int	// 命令索引
}

// 日志条目
type LogEntry struct {
	Command interface{}	// 具体的命令
	Term    int	// 日志所在任期
	Index   int	// 日志索引
}

// 命令的状态
type CommandState struct {
	Term  int	// 命令所在任期
	Index int	// 命令索引
}

const (
	FOLLOWER    = 0
	CANDIDATE   = 1
	LEADER      = 2
	CopyEntries = 3
	HeartBeat   = 4
)

type Raft struct {
	mu        sync.Mutex           // 互斥锁，用于共享变量的同步
	peers     []*rpcutil.ClientEnd // 服务器列表, 用来进行RPC通信的(ClientEnd实现参见package rpcutil)
	persister *Persister           // 持久化对象
	me        int                  // 当前服务器在服务器列表peers中的索引
	dead      int32                // 当前服务器是否宕机

	// 一个Raft节点必须要维护的一些数据结构
	currentTerm int        // 服务器最后一次知道的任期号（初始化为 0，持续递增）
	votedFor    int        // 在当前获得选票的候选人的 Id
	logs        []LogEntry // 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号

	// lastApplied的ID不能大于commitIndex的ID，因为只有commited状态的日志才能被提交到状态机执行
	commitIndex int // 已知的最大的已经被提交的日志条目的索引值
	lastApplied int // 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增）

	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一）
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值（初始化为0）

	identity     int	// 当前服务器的身份(follower, candidate, or leader)
	peersLen     int	// 服务器的总数
	hbCnt        int
	applyCh      chan ApplyMsg	// 用于提交命令的管道
	doAppendCh   chan int	// 用于AppendEntries的管道(用于heartbeat或者是appendEntries)
	applyCmdLogs map[interface{}]*CommandState	// 用于保存命令的字典
}

// 返回当前服务器状态（当前任期号以及当前服务器是否是leader）
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// 获取状态前需要加锁
	rf.mu.Lock()
	// defer表示等函数将return的值放入之后才开始执行
	defer rf.mu.Unlock()
	DPrintf("%v: 获取 %v 的状态", rf.currentTerm, rf.me)
	term = rf.currentTerm
	isleader = rf.identity == LEADER
	return term, isleader
}

// 将raft的状态进行持久化
// 需要持久化的数据有votedFor, currentTerm, 以及对应的日志logs
func (rf *Raft) persist() {
	// 写缓冲, 将编码的数据写入
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		encoder.Encode(rf.votedFor)
		encoder.Encode(rf.currentTerm)
		encoder.Encode(rf.logs)
		DPrintf("%v: %v 持久化了 votedFor=%v, logs 的最后一个是 %v",
			rf.currentTerm, rf.me, rf.votedFor, rf.logs[len(rf.logs) - 1])
	}()
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)
}

// 从之前保存的持久化状态恢复
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// 读缓冲. 读取data数据
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var votedFor, currentTerm int
	var logs []LogEntry
	err1 := decoder.Decode(&votedFor)
	err2 := decoder.Decode(&currentTerm)
	err3 := decoder.Decode(&logs)
	// 读取错误
	if err1 != nil || err2 != nil || err3 != nil {
		log.Fatalf("%v 恢复失败 err1=%v, err2=%v, err3=%v", rf.me, err1, err2, err3)
	} else {
		rf.votedFor = votedFor
		rf.currentTerm = currentTerm
		rf.logs = logs
		for i := range rf.logs {
			logEntry := rf.logs[i]
			rf.applyCmdLogs[logEntry.Command] = &CommandState{
				Term:  logEntry.Term,
				Index: logEntry.Index,
			}
		}
		//DPrintf("%v 恢复了 votedFor=%v, term=%v, logs=%v", rf.me, votedFor, currentTerm, logs)
	}
}

// AppendEntries函数所需要的参数
type AppendEntriesArgs struct {
	Term         int        // 领导人的任期号
	LeaderId     int        // 领导人的 Id，以便于跟随者重定向请求
	PrevLogIndex int        // 新的日志条目紧随之前的索引值
	PrevLogTerm  int        // PrevLogIndex 条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（表示心跳时为空；一次性发送多个是为了提高效率）
	LeaderCommit int        // 领导人已经提交的日志的索引值, 用于更新follower的commitIndex, 然后进行日志的提交
}

// AppendEntries函数的返回值
type AppendEntriesReply struct {
	Term         int  // 当前的任期号，用于领导人去更新自己
	Success      bool // 跟随者包含了匹配上 PrevLogIndex 和 PrevLogTerm 的日志时为真
	PrevLogIndex int // 返回跟随者的最长的PrevLogIndex, 当不匹配时, 也就是follower的log长度
}

// RequestVote函数的参数
type RequestVoteArgs struct {
	Term         int // 候选人的任期号
	CandidateId  int // 请求选票的候选人的 Id
	LastLogIndex int // 候选人的最后日志条目的索引值
	LastLogTerm  int // 候选人最后日志条目的任期号
	// 上面的任期号，日志索引以及日志任期号是用来判别服务器日志的更新状态
}

// RequestVote函数的返回值
type RequestVoteReply struct {
	Term        int  // 当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool // 候选人赢得了此张选票时为真
}

// RequestVote函数，用于candidate请求其他服务器进行投票, 如果超过半数为其投票, 则当前candidate变为leader
// 投票选取leader的条件是：leader的Term是最大的，同时日志的Index也是最大的
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 将返回参数中的Term设置为当前Raft节点的Term, 方便发起请求的服务器更新自己的Term
	reply.Term = rf.currentTerm
	// 如果当前节点的term大于args.term, 表明发起投票的candidate的节点日志比较老, 不投票, 直接返回
	if args.Term < rf.currentTerm {
		DPrintf("%v: %v 认为 %v 的投票请求 {term=%v, lastIdx=%v, lastTerm=%v} 过时了",
			rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm)
		return nil
	}

	// 如果当前节点的任期term小于args.Term, 当前服务器更新自己的任期号, 并投票
	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 接收到投票请求后发现自己过时，变回追随者，新的任期为 %v", rf.currentTerm, rf.me, args.Term)
		// 当前服务器更新自己的任期号
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.identity = FOLLOWER
	}

	// leader当选的条件是其对应的log是最新的, log最新的概念是最后一个log的任期号最大并且索引也是最大的
	// 比较两个Raft节点的日志的新旧程度
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLog := rf.logs[len(rf.logs)-1]
		// 比较两个节点的最后一条日志的任期号
		if args.LastLogTerm > lastLog.Term {
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		} else if args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index {
			// 比较两个节点最后一条日志的索引
			rf.identity = FOLLOWER
			rf.votedFor = args.CandidateId
			rf.hbCnt++
			reply.VoteGranted = true
		}
	}
	// 选举成功之后，将当前Raft节点的状态进行持久化
	go rf.persist()
	DPrintf("%v: %v 对 %v 的投票 {term=%v, lastIdx=%v, lastTerm=%v} 结果为 %v, votedFor=%v",
		rf.currentTerm, rf.me, args.CandidateId, args.Term, args.LastLogIndex, args.LastLogTerm, reply.VoteGranted, rf.votedFor)
	return nil
}

// AppendEntries函数，有两个功能，HeartBeat和log replication功能
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) error {
	// 在进行这些操作之前需要加锁，因为可能会有其他线程也来修改
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// PrevLogIndex表示待同步日志的上一个索引
	reply.PrevLogIndex = args.PrevLogIndex
	// 对于所有的Raft节点，如果发现自己的Term小于leader的任期，则更新自己的任期, 将votedFor变为args.LeaderId, 并将自己的状态变为FOLLOWER
	if args.Term > rf.currentTerm {
		DPrintf("%v: %v 收到条目增加请求后发现自己过时，变回追随者，新任期为 %v", rf.currentTerm, rf.me, args.Term)
		rf.currentTerm = args.Term
		rf.votedFor = args.LeaderId
		rf.identity = FOLLOWER
	}

	// 当前服务器的日志长度
	lenLogsCurrent := len(rf.logs)
	// 如果当前Raft节点的Term大于leader的Term，则直接返回
	if args.Term < rf.currentTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries) - 1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：已过时",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}
		return nil
	}

	// 如果PrevLogIndex大于当前服务器的日志长度, 或者不匹配
	if args.PrevLogIndex >= lenLogsCurrent || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		if len(args.Entries) > 0 {
			DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries) - 1],
				args.Term, args.LeaderCommit)
		} else {
			DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：不匹配",
				rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
		}
		//DPrintf("%v: %v 接收到 %v 的增加条目请求(%v)，结果为 %v", rf.currentTerm, rf.me, args.LeaderId, *args, reply.Success)
		if args.PrevLogIndex >= lenLogsCurrent {
			// 回传当前Raft节点的日志长度，重置PrevLogIndex参数, 用于leader进行更新nextIndex[i]
			reply.PrevLogIndex = lenLogsCurrent
			return nil
		}

		// 如果不匹配的话，将当前term的日志全都删除掉, 然后设置PrevLogIndex
		i := args.PrevLogIndex
		term := rf.logs[i].Term
		for i--; i >= 0 && rf.logs[i].Term == term; i-- {
		}
		reply.PrevLogIndex = i + 1
		return nil
	}

	// 如果PrevLogIndex匹配成功了, 就开始复制日志，为了提高效率，这里PrevLogIndex之后第一个不匹配的，然后进行复制
	var deleteLogEntries []LogEntry
	idx1 := args.PrevLogIndex + 1
	idx2 := len(args.Entries) - 1
	for idx1 < lenLogsCurrent && idx2 >= 0 {
		log1 := &rf.logs[idx1]
		log2 := args.Entries[idx2]
		if log1.Term != log2.Term || log1.Index != log2.Index {
			deleteLogEntries = rf.logs[idx1:]
			rf.logs = rf.logs[:idx1]
			break
		}
		idx1++
		idx2--
	}
	for i := 0; i < len(deleteLogEntries); i++ {
		delete(rf.applyCmdLogs, deleteLogEntries[i].Command)
	}
	// 将日志复制到当前节点
	for idx2 >= 0 {
		logEntry := args.Entries[idx2]
		rf.logs = append(rf.logs, logEntry)
		rf.applyCmdLogs[logEntry.Command] = &CommandState{
			Term:  logEntry.Term,
			Index: logEntry.Index,
		}
		idx2--
	}

	// 更新当前server的commitIndex, 如果leader已经commit了, 则follower就可以将对应的命令apply到状态机
	if rf.commitIndex < args.LeaderCommit {
		idx := len(rf.logs) - 1
		if args.LeaderCommit <= idx {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = idx
		}
		// 更新commitIndex之后，将commited状态的命令Apply到状态机
		go rf.apply()
	}

	rf.hbCnt++
	reply.Success = true
	// 日志复制之后，需要进行状态的持久化操作
	go rf.persist()
	if len(args.Entries) > 0 {
		DPrintf("%v: %v 接收到 %v 的复制请求 {{%v ~ %v} term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Entries[0], args.Entries[len(args.Entries)-1],
			args.Term, args.LeaderCommit)
	} else {
		DPrintf("%v: %v 接收到 %v 的复制请求 {term=%v leaderCommit=%v}，结果为：成功",
			rf.currentTerm, rf.me, args.LeaderId, args.Term, args.LeaderCommit)
	}
	//DPrintf("%v: %v 接收到 %v 的增加条目(%v)请求，结果为 %v", rf.currentTerm, rf.me, args.LeaderId, *args, reply.Success)

	return nil
}

// RequestVote的RPC接口, 请求ID为server的投票
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntries的RPC接口, 请求ID为server的日志复制和HeartBeat
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Raft节点执行命令, 调用此接口来是Raft节点执行对应的命令
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = !rf.killed() && rf.identity == LEADER
	// 当期节点的任期Term
	term = rf.currentTerm
	// Raft中所有的操作都是需要进过leader进行操作的
	// 如果是leader, 就进行相应的操作
	if isLeader {
		// 如果是相同的操作(即之前出现过的已经apply的操作), 就直接返回, 不需要再进行其他的操作
		// 例如两个相同的set "a" "apple", 就不用进行多余的操作
		if commandState, has := rf.applyCmdLogs[command]; has {
			return commandState.Index, commandState.Term, isLeader
		}

		// leader进行相应的操作
		index = len(rf.logs)
		// 新建日志条目
		logEntry := LogEntry{
			Command: command,
			Term:    term,
			Index:   index,
		}
		rf.logs = append(rf.logs, logEntry)
		// 将当前操作放到字典中, 记录已经做过相应的操作
		rf.applyCmdLogs[command] = &CommandState{
			Term:  term,
			Index: index,
		}
		// 每次命令做完之后就新开一个线程做持久化处理
		go rf.persist()
		// 将CopyEntries放到通道doAppendCh中, 从doAppendCh中取出相应的标记来做AppendEntries操作
		go func() {
			// 表示需要进行日志的复制, 放到管道中
			rf.doAppendCh <- CopyEntries
		}()
		DPrintf("-----%v 是领导，增加了新的条目为 {%v %v %v}", rf.me, command, term, index)
	}

	return index, term, isLeader
}

func (rf *Raft) Kill() {
	// 原子操作, 设置Raft节点的dead状态为1
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("----- %v 结束 -----", rf.me)
	func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		DPrintf("%v: %v 的日志内容：%v", rf.currentTerm, rf.me, rf.logs)
	}()
}

// 判断当前Raft节点是否宕机
func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 向所有follower发送日志
func (rf *Raft) sendLogEntry(flag int) {
	rf.mu.Lock()
	// 如果当前Raft节点不是leader, 直接返回
	if rf.identity != LEADER {
		rf.mu.Unlock()
		return
	}

	// 构造AppendEntries参数
	argsTemplate := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}

	loglen := len(rf.logs)
	//if flag == CopyEntries {
	DPrintf("%v: %v 开始复制，最后一个条目为 %v，最后提交的索引为 %v",
		rf.currentTerm, rf.me, rf.logs[loglen - 1], rf.commitIndex)
	//}
	rf.mu.Unlock()

	resultCh := make(chan int, rf.peersLen)
	// WaitGroup用来等待一组线程的结束
	wg := sync.WaitGroup{}
	// 增加引用计数, 应该在创建新的协程之前调用
	wg.Add(1)
	// 协程做完之后调用Done函数, 将引用计数减1
	defer wg.Done()
	go func() {
		// Wait阻塞, 直到引用计数为0, 当引用计数为0时, Wait方法阻塞等待的所有协程都会返回
		wg.Wait()
		close(resultCh)
	}()

	// 遍历所有的节点, 向所有follower发送日志
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		// 引用计数加1, 表明当前有多少协程在运行
		wg.Add(1)
		// goroutine, 每个节点的复制都重新一个协程来做
		go func(i int) {
			// 协程退出时将Done减为1
			defer wg.Done()
			args := argsTemplate
			// PrevLogIndex初始化
			preIdx := func() int {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				curNextIndex := loglen - 1
				// 根据nextIndex[i], 将nextIndex[i]之后的日志加入到args.Entries中
				for ; curNextIndex >= rf.nextIndex[i]; curNextIndex-- {
					args.Entries = append(args.Entries, rf.logs[curNextIndex])
				}
				return curNextIndex
			}()

			// 不断减少preIdx来进行AppendEntries的RPC通信, leader和follower之间日志的对齐
			for preIdx >= 0 {
				rf.mu.Lock()
				if rf.identity != LEADER || preIdx >= len(rf.logs) {
					rf.mu.Unlock()
					// 结果为-2
					resultCh <- -2
					break
				}
				args.PrevLogIndex = preIdx
				args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
				rf.mu.Unlock()

				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(i, &args, &reply)
				if !ok {
					// 操作错误
					resultCh <- -2
					break
				}

				// 返回的任期号Term大于当前节点的任期号
				if reply.Term > args.Term {
					resultCh <- reply.Term
					break
				}

				// 如果复制成功, 则重新设置第i个服务器的nextIndex[i]值
				if reply.Success {
					rf.mu.Lock()
					// 当前的日志已经全部复制到follower, 设置相应的nextIndex
					rf.nextIndex[i] = loglen
					rf.mu.Unlock()
					// 将结果存放到result通道中
					resultCh <- -1
					/*
						if len(args.Entries) > 0 {
							DPrintf("%v: %v 向 %v 发送请求 {{%v ~ %v} commit=%v perTerm=%v }，复制成功",
								args.Term, rf.me, i, args.Entries[0], args.Entries[len(args.Entries) - 1], args.LeaderCommit, args.PrevLogTerm)
						} else {
							DPrintf("%v: %v 向 %v 发送请求 {{} commit=%v perTerm=%v }，复制成功",
								args.Term, rf.me, i, args.LeaderCommit, args.PrevLogTerm)
						}
					*/
					break
				} else {
					/*
						if len(args.Entries) > 0 {
							DPrintf("%v: %v 向 %v 发送请求 {{%v ~ %v} commit=%v perTerm=%v}，复制失败",
								args.Term, rf.me, i, args.Entries[0], args.Entries[len(args.Entries) - 1], args.LeaderCommit, args.PrevLogTerm)
						} else {
							DPrintf("%v: %v 向 %v 发送请求 {{} commit=%v perTerm=%v }，复制失败",
								args.Term, rf.me, i, args.LeaderCommit, args.PrevLogTerm)
						}
					*/
					rf.mu.Lock()
					// 如果当前preIdx大于节点日志的长度，直接返回
					if preIdx >= len(rf.logs) {
						rf.mu.Unlock()
						break
					}
					// 设置nextIndex[i]为follower返回的PrevLogIndex
					rf.nextIndex[i] = reply.PrevLogIndex
					// 然后将为未复制的日志重新进行复制
					for ; preIdx >= reply.PrevLogIndex; preIdx-- {
						args.Entries = append(args.Entries, rf.logs[preIdx])
					}
					rf.mu.Unlock()
				}
			}
		}(i)
	}

	// 自己已经复制，所以初始为1
	grantedCount := 1
	notGrantedCount := 0
	// 需要 n / 2 + 1个服务器复制成功才表示复制成功
	tgt := rf.peersLen / 2
	for finish := 1; finish < rf.peersLen; finish++ {
		// 如果resultCh中没有数据, 则会一直阻塞, 直到有足够的follower完成复制
		result := <-resultCh

		rf.mu.Lock()
		if rf.identity != LEADER {
			rf.mu.Unlock()
			break
		}
		// 如果当前任期改变了, 则表示进入了新的任期, 直接返回
		if rf.currentTerm != argsTemplate.Term {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()

		// result等于-1表示复制成功
		if result == -1 {
			// 表示成功复制到一个follower节点上
			grantedCount++
			// 如果超过n / 2 + 1个节点复制成功, 则将其标识为commited状态
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					commit := loglen - 1
					// 如果当前commitIdx小于commit, 则将当前已经commit的日志apply到状态机进行执行
					if rf.identity == LEADER && commit < len(rf.logs) && commit > rf.commitIndex {
						rf.commitIndex = commit
						go rf.apply()
						//if flag == CopyEntries {
						DPrintf("%v: %v 提交成功，提交的最大的索引为 %v，最后复制过去的是 %v",
							argsTemplate.Term, rf.me, rf.commitIndex, rf.logs[commit])
						//}
					}
				}()
				break
			}
		} else if result == -2 {	// 复制失败
			// notGrantedCount加1
			notGrantedCount++
			// 如果复制失败的节点数大于n / 2 + 1, 则表示当前提交失败, 当前leader变为follower
			if notGrantedCount > tgt {
				//if flag == CopyEntries {
				DPrintf("%v: %v 提交失败，准备提交的索引为 %v，退回追随者", argsTemplate.Term, rf.me, loglen-1)
				//}
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					rf.identity = FOLLOWER
				}()
				break
			}
		} else if result > argsTemplate.Term {	// 如果返回的Term大于当前Term
			func() {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				// 如果当前的任期号Term小于返回的任期号, 则将当前节点状态后退为follower, 重置任期号
				if rf.currentTerm < result {
					rf.currentTerm = result
					rf.votedFor = -1
					rf.identity = FOLLOWER
					DPrintf("%v: %v 收到条目增加响应后发现自己过时，变成追随者，新任期为 %v",
						argsTemplate.Term, rf.me, rf.currentTerm)
				}
			}()
			break
		} else {
			panic("出现一个意外的值： result=" + string(result))
		}
	}
}

// 设置当前Raft节点为FOLLOWER
func (rf *Raft) setToFollower() {
	//DPrintf("%v: 将 %v 变为追随者", rf.currentTerm, rf.me)
	rf.identity = FOLLOWER
}

// 将commited状态的命令提交到状态机执行
func (rf *Raft) apply() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 如果最近一次提交的日志索引小于状态机执行日志索引，则是异常状态
	// 因为只有commited状态的日志才能被apply到状态机进行执行
	if rf.commitIndex < rf.lastApplied {
		log.Fatalf("%v: %v 调用 apply()： commitIndex(%v) < lastApplied(%v)",
			rf.currentTerm, rf.me, rf.currentTerm, rf.lastApplied)
	}
	// 如果最近一次提交的日志索引和状态机执行的日志索引相等，则没有需要执行的日志
	if rf.commitIndex == rf.lastApplied {
		return
	}
	// 只有commitIdx大于lastApplied的时候才是正常状态
	// 此时执行循环，将所有commited状态的日志全部apply状态机进行执行
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		logEntry := rf.logs[rf.lastApplied]
		// 将ApplyMsg存到applyCh管道中
		// KV数据库从applyCh通道中取命令执行
		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      logEntry.Command,
			CommandIndex: logEntry.Index,
		}
		// DPrintf("%v: %v 应用第 %v 个, 命令为 %v", rf.currentTerm, rf.me, rf.lastApplied, logEntry.Command)
	}
	DPrintf("%v: %v 最后应用的是 %v, logs 里最后一个是 %v",
		rf.currentTerm, rf.me, rf.logs[rf.lastApplied], rf.logs[len(rf.logs) - 1])
}

// 生成一个随机超时时间
func randomTimeout(min, max int) int {
	return rand.Intn(max-min) + min
}

// 返回一个Raft节点实例
/*
 参数：
	peers: 		表示集群中的所有服务器
	me:			表示当前服务器在peers中的索引
	persister:	一个持久化对象
	applyCh:	Apply通道, 将需要状态机Apply的命令传入到applyCh通道中, 数据库从applyCh通道中取命令行
 返回:
	一个Raft实例
 */
func Make(peers []*rpcutil.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	fmt.Printf("----- %v 开始 -----\n", rf.me)
	rf.votedFor = -1
	rf.peersLen = len(peers)
	rf.logs = append(rf.logs, LogEntry{
		Command: "start",
		Term:    0,
		Index:   0,
	})
	// 首先将当前服务器设置为Follower
	rf.setToFollower()
	rf.commitIndex = 0
	// 创建nextIndex, int数组, 数组长度为服务器的数量
	rf.nextIndex = make([]int, rf.peersLen)
	// 创建matchIndex, int数组, 数组长度为服务器的数量
	rf.matchIndex = make([]int, rf.peersLen)
	rf.applyCh = applyCh
	rf.doAppendCh = make(chan int, 256)
	// 存储命令的字典
	rf.applyCmdLogs = make(map[interface{}]*CommandState)
	// 随机时间选取
	rand.Seed(time.Now().UnixNano())

	// 开启一个协程, 不断做leader选举
	go func() {
		for {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			switch rf.identity {
			// 如果是follower
			case FOLLOWER:
				oldCnt := rf.hbCnt
				rf.mu.Unlock()
				// 设置一个随机超时时间
				timeout := time.Duration(randomTimeout(700, 1000)) * time.Millisecond
				time.Sleep(timeout)
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.hbCnt == oldCnt {
						// 将身份变为candidate
						rf.identity = CANDIDATE
						rf.currentTerm++
						rf.votedFor = rf.me
						go rf.persist()
						fmt.Printf("%v: %v 的心跳超时 (%v)", rf.currentTerm, rf.me, timeout)
					}
				}()
			case CANDIDATE:
				fmt.Printf("%v: %v 开始竞选", rf.currentTerm, rf.me)
				rf.mu.Unlock()

				wonCh := make(chan int, 2)
				// WaitGroup类, 用来监控协程的运行状态
				wg := sync.WaitGroup{}
				wg.Add(1)
				go func() {
					// Wait()函数阻塞直到引用计数为0
					wg.Wait()
					close(wonCh)
				}()

				wg.Add(1)
				// 进行选举操作
				go rf.leaderElection(wonCh, &wg)

				// 设置一个超时时间
				timeout := time.Duration(randomTimeout(1000, 1400)) * time.Millisecond
				wg.Add(1)
				go func() {
					// 这里进行超时操作
					time.Sleep(timeout)
					wonCh <- 2
					wg.Done()
				}()

				// 如果最后wonCh通道返回的结果是2, 说明超时时间到了, 但是选举依然没有完成, 进行新一轮的选举
				res := <-wonCh
				// 选举超时
				if 2 == res {
					fmt.Printf("%v: %v 竞选超时 (%v)\n", rf.currentTerm, rf.me, timeout)
					rf.mu.Lock()
					rf.votedFor = rf.me
					rf.currentTerm++
					rf.mu.Unlock()
					go rf.persist()
				}
				wg.Done()
			default:
				fmt.Printf("当前Leader是: %d\n", rf.me)
				rf.mu.Unlock()
				// 如果是leader, 则发送心跳
				rf.doAppendCh <- HeartBeat
				time.Sleep(time.Millisecond * 100)
			}
		}
	}()

	// 开启一个协程, 做日志的发送(即AppendEntries, 两个功能, HeartBeat和添加日志AppendEntries)
	go func() {
		// 不断循环做sendLogEntry操作
		// 函数调用关系: Make(返回一个Raft节点实例) -> sendLogEntry(循环给每个follower节点发送HeartBeat信号) -> sendAppendEntries -> AppendEntries
		for {
			if rf.killed() {
				return
			}
			// 从doAppendCh管道中读取leader需要和follower进行的通信种类
			rf.sendLogEntry(<-rf.doAppendCh)
		}
	}()

	// 从持久化对象中读取状态
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// leader选举函数
func (rf *Raft) leaderElection(wonCh chan int, wgp *sync.WaitGroup) {
	defer wgp.Done()

	// 创建RPC函数RequestVote所需的参数
	args := &RequestVoteArgs{}
	args.CandidateId = rf.me

	rf.mu.Lock()
	lcterm := rf.currentTerm
	args.Term = rf.currentTerm
	// LastLogIndex
	args.LastLogIndex = len(rf.logs) - 1
	// LastLogTerm
	args.LastLogTerm = rf.logs[args.LastLogIndex].Term
	rf.mu.Unlock()

	c := make(chan *RequestVoteReply, rf.peersLen)
	wg := sync.WaitGroup{}
	wg.Add(1)
	defer wg.Done()
	go func() {
		wg.Wait()
		// 异步操作, 最后需要关闭管道c, 否则会一直阻塞
		close(c)
	}()

	// 遍历所有peers, 请求投票将自己变为leader
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		wg.Add(1)
		// RPC操作一定不能加锁, 新建协程来做, 一个并行的操作
		go func(i int) {
			defer wg.Done()
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(i, args, &reply)
			if !ok {
				c <- nil
				//DPrintf("%v: %v 向 %v 发起投票请求 {term=%v, lastIdx=%v, lastTerm=%v} 超时",
				//	args.Term, rf.me, i, args.Term, args.LastLogIndex, args.LastLogTerm)
				return
			}
			c <- &reply
		}(i)
	}

	grantedCount := 1
	notgrantedCount := 0
	tgt := rf.peersLen / 2
	// 判断为自己投票的Raft节点是否超过n / 2 + 1
	for finish := 1; finish < rf.peersLen; finish++ {
		// 获取RequestVote的Reply数据
		reply := <-c

		rf.mu.Lock()
		// 如果当前自己的任期号改变了, 说明进入了新的任期, 直接返回
		if rf.currentTerm != lcterm {
			rf.mu.Unlock()
			break
		}
		// 如果状态不是CANDIDATE状态, 说明退化为FOLLOWER了, 直接返回
		if rf.identity != CANDIDATE {
			rf.mu.Unlock()
			break
		}
		// 中间这些都是会改变的信息, 需要加锁
		rf.mu.Unlock()

		if reply == nil {
			notgrantedCount++
			continue
		}

		// 票数加1
		if reply.VoteGranted {
			grantedCount++
			// 超过半数为自己投票, 则自己当选为leader
			if grantedCount > tgt {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					// 将自己设置为leader
					if rf.identity != LEADER {
						rf.identity = LEADER
						nextIndex := len(rf.logs)
						// 初始化nextIndex为len(rf.logs)
						for i := range rf.nextIndex {
							rf.nextIndex[i] = nextIndex
						}
						// 同时发送HeartBeat信号(其实和AppendEntries一样, 都是一个接口)
						rf.doAppendCh <- HeartBeat
						// go rf.sendLogEntry(HeartBeat)
						DPrintf("%v: %v 赢得了选举，变为领导者，发送了初始心跳",
							args.Term, rf.me)
					}
				}()
				// 选举成功标志
				wonCh <- 1
				break
			}
		} else {
			// 如果当前任期小于返回的任期, 当前节点变为follower, 停止选举操作
			if args.Term < reply.Term {
				func() {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if rf.currentTerm < reply.Term {
						DPrintf("%v: %v 发送投票后发现自己过时，变回追随者", args.Term, rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identity = FOLLOWER
						// 选举失败标志
						wonCh <- 1
					}
				}()
				break
			}

			notgrantedCount++
			// 超过半数不为自己投票, 没有必要再检查其他节点
			if notgrantedCount > tgt {
				break
			}
		}
	}
}
