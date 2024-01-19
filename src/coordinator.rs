//! 
//! coordinator.rs
//! Implementation of 2PC coordinator
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use coordinator::rand::prelude::*;use std::thread;
use std::sync::{Arc};
use std::sync::Mutex;
use std::sync::mpsc;
use std::sync::mpsc::channel;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32};
use std::sync::atomic::{AtomicBool, Ordering};
use message::ProtocolMessage;
use message::MessageType;
use message::RequestStatus;
use message;
use oplog;
use client;
use participant;

/// CoordinatorState
/// States for 2PC state machine
/// 
/// TODO: add and/or delete!
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordinatorState {    
    Quiescent,          
    Active,
}

/// Coordinator
/// struct maintaining state for coordinator
#[derive(Debug)]
pub struct Coordinator {
    state: CoordinatorState,
    log: oplog::OpLog,
    msg_success_prob: f64,
    ops_success_prob: f64,
    running: Arc<AtomicBool>,
    pub client_ports: (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>),
    pub part_ports: (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>),
    pub client_data: HashMap<String, (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>)>,
    pub participant_data: HashMap<String, (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>)>,
    num_clients: i32,
    num_participants: i32,
    all_voted: bool,
    num_req_handled: i32,
    total_req: i32,
    pub successful: i32,
    pub failed: i32, 
    pub unknown: i32,
}

///
/// Coordinator
/// implementation of coordinator functionality
/// Required:
/// 1. new -- ctor
/// 2. protocol -- implementation of coordinator side of protocol
/// 3. report_status -- report of aggregate commit/abort/unknown stats on exit.
/// 4. participant_join -- what to do when a participant joins
/// 5. client_join -- what to do when a client joins
/// 
impl Coordinator {

    ///
    /// new()
    /// Initialize a new coordinator
    /// 
    /// <params>
    ///     logpath: directory for log files --> create a new log there. 
    ///     r: atomic bool --> still running?
    ///     msg_success_prob --> probability sends succeed
    ///
    pub fn new(
        logpath: String, 
        r: Arc<AtomicBool>, 
        msg_success_prob: f64,
        ops_success_prob: f64,
        total_requests: i32) -> Coordinator {

        Coordinator {
            state: CoordinatorState::Quiescent,
            log: oplog::OpLog::new(logpath),
            msg_success_prob: msg_success_prob,
            ops_success_prob: ops_success_prob,
            running: r,
            client_data: HashMap::new(),
            participant_data: HashMap::new(),
            num_clients: 0,
            num_participants: 0,
            client_ports: (channel()),
            part_ports: (channel()),
            all_voted: true,
            num_req_handled: 0,
            total_req: total_requests,
            successful: 0,
            failed: 0,
            unknown: 0,
        }
    }

    /// 
    /// participant_join()
    /// handle the addition of a new participant
    /// HINT: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn participant_join(&mut self, name: String, logpathbase: &String) -> participant::Participant {

        assert!(self.state == CoordinatorState::Quiescent);

        let (p_tx, coord_rx): (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>) = channel();
        let (coord_tx, p_rx): (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>) = channel();
        let part = participant::Participant::new(self.num_participants, self.num_participants.to_string(), p_tx, 
                    p_rx, format!("{}/participant_{}.log", logpathbase, self.num_participants), self.running.clone(), self.ops_success_prob, self.msg_success_prob);
        
        self.num_participants = self.num_participants + 1;
        self.participant_data.insert(name, (coord_tx, coord_rx));

        part
    }

    /// 
    /// client_join()
    /// handle the addition of a new client
    /// HINTS: keep track of any channels involved!
    /// HINT: you'll probably need to change this routine's 
    ///       signature to return something!
    ///       (e.g. channel(s) to be used)
    /// 
    pub fn client_join(&mut self, name: String) -> client::Client  {

        assert!(self.state == CoordinatorState::Quiescent);
        

        let (client_tx, coord_rx): (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>) = channel();
        let (coord_tx, client_rx): (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>) = channel();
        let client = client::Client::new(self.num_clients, (self.num_clients).to_string(), client_tx, client_rx, self.running.clone());

        self.num_clients = self.num_clients + 1;
        self.client_data.insert(name, (coord_tx, coord_rx));

        client
    }

    /// 
    /// send()
    /// send a message, maybe drop it
    /// HINT: you'll need to do something to implement 
    ///       the actual sending!
    /// 
    pub fn send(&self, sender: &Sender<ProtocolMessage>, pm: ProtocolMessage, panic: &mut bool) -> bool {

        let x: f64 = random();
        let mut result: bool = false;
        if x < self.msg_success_prob {
            let res = sender.send(pm.clone());
            match res {
                Ok(_val) => result = true,
                Err(_err) => {
                    *panic = true;
                },
            }
        } else {
            // don't send anything!
            // (simulates failure)
            result = false;
        }
        result
    }     

    /// 
    /// recv_request()
    /// receive a message from a client
    /// to start off the protocol.
    /// 
    pub fn recv_request(&mut self, found: &mut bool) -> (Option<ProtocolMessage>, String) {

        let mut result = Option::None;
        assert!(self.state == CoordinatorState::Quiescent);        
        trace!("coordinator::recv_request...");

        for _i in 0..10 {
            for (key, val) in self.client_data.iter() {
                // let rx = rec as Receiver<message::ProtocolMessage>;
                let pm = val.1.recv_timeout(Duration::from_millis(10));
                match pm {
                    Ok(val) => {
                        result = Some(val);
                        *found = true;
                        // break;
                        trace!("leaving coordinator::recv_request");
                        return (result, key.clone());
                    }
                    Err(err) => {
                        match err {
                            mpsc::RecvTimeoutError::Timeout => {
                                continue;
                            }
                            mpsc::RecvTimeoutError::Disconnected => {
                                // let map = &mut self.client_data;
                                // map.remove(key);
                                // // self.client_data.remove(key);
                                continue;
                            }
                        }
                    }
                }
            }
        }

        *found = false;
        trace!("leaving coordinator::recv_request");
        (result, String::from(""))
    }        

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {
        let successful_ops: i32 = self.successful; // TODO!
        let failed_ops: i32 = self.failed; // TODO!
        let unknown_ops: i32 = self.unknown; // TODO! 
        println!("coordinator:\tC:{}\tA:{}\tU:{}", successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the coordinator side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {

        let mut active;
        while self.num_req_handled != self.total_req {
            active = self.running.load(Ordering::SeqCst);
            if active {
                let mut found = false;
                let res = self.recv_request(&mut found);
                if found {
                    let pm = res.0.unwrap();
                    self.log.append(pm.mtype, pm.txid, pm.senderid, pm.opid);
                    assert_eq!(pm.mtype, MessageType::ClientRequest);
                    let prepare = ProtocolMessage::generate(MessageType::CoordinatorPropose, pm.txid, format!("coordinator"), pm.opid);
                    self.log.append(prepare.mtype, prepare.txid, prepare.senderid.clone(), prepare.opid);
                    for (key, val) in self.participant_data.iter() {
                        let mut panic = false;
                        let mut res = self.send(&val.0, prepare.clone(), &mut panic);
                        if !res && !panic {
                            while !res {
                                res = self.send(&val.0, prepare.clone(), &mut panic);
                                if panic {break; }
                            }
                        }
                    }

                    // wait for ready from all p
                    for (_key, val) in self.participant_data.iter() {
                        let res = val.1.recv_timeout(Duration::from_millis(500));
                        match res {
                            Ok(value) => {
                                if value.mtype == MessageType::ParticipantVoteAbort {
                                    self.all_voted = false;
                                } else {
                                    continue;
                                }
                            }
                            Err(_err) => {
                                self.all_voted = false;
                            }
                        }
                    }

                    // send global to all part
                    let mes;
                    if self.all_voted {
                        mes = message::ProtocolMessage::generate(MessageType::CoordinatorCommit, pm.txid, format!("coordinator"), pm.opid);
                        self.successful = self.successful + 1;
                        // obviously need to log
                    } else {
                        mes = message::ProtocolMessage::generate(MessageType::CoordinatorAbort, pm.txid, format!("coordinator"), pm.opid);
                        self.failed = self.failed + 1;
                    }

                    self.log.append(mes.mtype, mes.txid, mes.senderid.clone(), mes.opid);
                    for (key, val) in self.participant_data.iter() {
                        let mut panic = false;
                        let mut res = self.send(&val.0, mes.clone(), &mut panic);
                        if !res && !panic {
                            while !res {
                                res = self.send(&val.0, mes.clone(), &mut panic);
                                if panic {break; }
                            }
                        }
                    }

                    let cl_res;
                    if self.all_voted {
                        cl_res = message::ProtocolMessage::generate(MessageType::ClientResultCommit, pm.txid, format!("coordinator"), pm.opid);
                    } else {
                        cl_res = message::ProtocolMessage::generate(MessageType::ClientResultAbort, pm.txid, format!("coordinator"), pm.opid);
                    }
                    self.log.append(cl_res.mtype, cl_res.txid, cl_res.senderid.clone(), cl_res.opid);
                    let cl_send = self.client_data.get(&res.1).unwrap();
                    let mut panic = false;
                    let mut res = self.send(&cl_send.0, cl_res.clone(), &mut panic);
                    if !res && !panic {
                        while !res {
                            res = self.send(&cl_send.0, cl_res.clone(), &mut panic);
                            if panic {break; }
                        }
                    }

                    self.all_voted = true;
                    self.num_req_handled = self.num_req_handled + 1;
                }
            } else {
                break;
            }
        }

        active = self.running.load(Ordering::SeqCst);
        if active {
            let exit = message::ProtocolMessage::generate(MessageType::CoordinatorExit, -1, format!("coordinator"), -1);
            for (key, val) in self.participant_data.iter() {
                let mut panic = false;
                let mut res = self.send(&val.0, exit.clone(), &mut panic);
                if !res && !panic {
                    while !res {
                        res = self.send(&val.0, exit.clone(), &mut panic);
                        if panic {break; }
                    }
                }
            }
            for (key, val) in self.client_data.iter() {
                let mut panic = false;
                let mut res = self.send(&val.0, exit.clone(), &mut panic);
                if !res && !panic {
                    while !res {
                        res = self.send(&val.0, exit.clone(), &mut panic);
                        if panic {break; }
                    }
                }
            }
        }

        self.running.store(false, Ordering::SeqCst);
        self.report_status();

                                
    }
}
