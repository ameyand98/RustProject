//! 
//! participant.rs
//! Implementation of 2PC participant
//! 
extern crate log;
extern crate stderrlog;
extern crate rand;
use participant::rand::prelude::*;
use std::sync::mpsc;
use std::sync::mpsc::{Sender, Receiver};
use std::time::Duration;
use std::sync::atomic::{AtomicI32};
use std::sync::{Arc};
use std::sync::atomic::{AtomicBool, Ordering};
use message;
use message::MessageType;
use message::ProtocolMessage;
use message::RequestStatus;
use std::collections::HashMap;
use std::thread;
use oplog;

/// 
/// ParticipantState
/// enum for participant 2PC state machine
/// 
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParticipantState {    
    Quiescent,          
    // TODO ...
}

///
/// Participant
/// structure for maintaining per-participant state 
/// and communication/synchronization objects to/from coordinator
/// 
#[derive(Debug)]
pub struct Participant {    
    pub id: i32,
    id_str: String,
    state: ParticipantState,
    log: oplog::OpLog,
    op_success_prob: f64,
    msg_success_prob: f64,
    pub ports: (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>),
    running: Arc<AtomicBool>,
    pub successful: i32,
    pub failed: i32, 
    pub unknown: i32,
}

///
/// Participant
/// implementation of per-participant 2PC protocol
/// Required:
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown for each participant
/// 3. pub fn protocol() -- implements participant side protocol
///
impl Participant {

    /// 
    /// new()
    /// 
    /// Return a new participant, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->participant and participant->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor. There are other
    ///       ways to communicate this, of course. 
    /// 
    pub fn new(
        i: i32, is: String, 
        tx: Sender<ProtocolMessage>, 
        rx: Receiver<ProtocolMessage>, 
        logpath: String,
        r: Arc<AtomicBool>,
        f_success_prob_ops: f64,
        f_success_prob_msg: f64) -> Participant {

        Participant {
            id: i,
            id_str: is,
            log: oplog::OpLog::new(logpath),
            op_success_prob: f_success_prob_ops,
            msg_success_prob: f_success_prob_msg,
            state: ParticipantState::Quiescent,
            ports: (tx, rx),
            running: r,
            successful: 0,
            failed: 0,
            unknown: 0,
        }   
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator.
    /// This variant can be assumed to always succeed.
    /// You should make sure your solution works using this 
    /// variant before working with the send_unreliable variant.
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending.
    /// 
    pub fn send(&mut self, pm: ProtocolMessage) -> bool {
        let result;

        let res = self.ports.0.send(pm);
        match res {
            Ok(_val) => result = true,
            Err(_err) => result = false,
        }

        result
    }

    ///
    /// send()
    /// Send a protocol message to the coordinator, 
    /// with some probability of success thresholded by the 
    /// command line option success_probability [0.0..1.0].
    /// This variant can be assumed to always succeed
    /// 
    /// HINT: you will need to implement something that does the 
    ///       actual sending, but you can use the threshold 
    ///       logic in this implementation below. 
    /// 
    pub fn send_unreliable(&mut self, pm: ProtocolMessage) -> bool {
        let x: f64 = random();
        let result: bool;
        if x < self.msg_success_prob {
            result = self.send(pm);
        } else {
            result = false;
        }
        result
    }    

    /// 
    /// perform_operation
    /// perform the operation specified in the 2PC proposal,
    /// with some probability of success/failure determined by the 
    /// command-line option success_probability. 
    /// 
    /// HINT: The code provided here is not complete--it provides some
    ///       tracing infrastructure and the probability logic. 
    ///       Your implementation need not preserve the method signature
    ///       (it's ok to add parameters or return something other than 
    ///       bool if it's more convenient for your design).
    /// 
    pub fn perform_operation(&mut self, request: &Option<ProtocolMessage>) -> bool {

        trace!("participant::perform_operation");

        let mut result: RequestStatus = RequestStatus::Unknown;
        let pm: &ProtocolMessage = request.as_ref().unwrap();

        let x: f64 = random();
        if x > self.op_success_prob {
            match pm.mtype {
                MessageType::CoordinatorPropose => {
                    self.log.append(pm.mtype, pm.txid, pm.senderid.clone(), pm.opid);
                    let vabort = ProtocolMessage::generate(MessageType::ParticipantVoteAbort, pm.txid, format!("participant_{}", self.id), pm.opid);
                    self.log.append(vabort.mtype.clone(), vabort.txid.clone(), vabort.senderid.clone(), vabort.opid.clone());
                    let res;
                    if self.msg_success_prob == 1.0 {
                        res = self.send(vabort);
                    } else {
                        res = self.send_unreliable(vabort);
                    }
                    result = RequestStatus::Aborted;
                    let reply = self.ports.1.recv().unwrap();
                    match reply.mtype {
                        MessageType::CoordinatorAbort => {
                            self.log.append(reply.mtype, reply.txid, reply.senderid, reply.opid);
                            self.failed = self.failed + 1;
                            result = RequestStatus::Aborted;
                        }
                        _ => {self.unknown = self.unknown + 1},
                    }
                },
                _ => {},
            }

        } else {
            match pm.mtype {
                MessageType::CoordinatorPropose => {
                    self.log.append(pm.mtype, pm.txid, pm.senderid.clone(), pm.opid);
                    let vcommit = ProtocolMessage::generate(MessageType::ParticipantVoteCommit, pm.txid, format!("participant_{}", self.id), pm.opid);
                    self.log.append(vcommit.mtype.clone(), vcommit.txid.clone(), vcommit.senderid.clone(), vcommit.opid.clone());
                    let res;
                    if self.msg_success_prob == 1.0 {
                        res = self.send(vcommit);
                    } else {
                        res = self.send_unreliable(vcommit);
                    }
                    // wait for phase 2
                    let reply = self.ports.1.recv().unwrap();
                    match reply.mtype {
                        MessageType::CoordinatorCommit => {
                            self.log.append(reply.mtype, reply.txid, reply.senderid, reply.opid);
                            self.successful = self.successful + 1;
                            result = RequestStatus::Committed;
                        }
                        MessageType::CoordinatorAbort => {
                            self.log.append(reply.mtype, reply.txid, reply.senderid, reply.opid);
                            self.failed = self.failed + 1;
                            result = RequestStatus::Aborted;
                        }
                        _ => {self.unknown = self.unknown + 1},
                    }

                },
                MessageType::CoordinatorExit => {
                    self.running.store(false, Ordering::SeqCst);
                },
                _ => {},
            }

        }

        trace!("exit participant::perform_operation");
        result == RequestStatus::Committed
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this coordinator before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: maintain actual stats!
        let global_successful_ops: i32 = self.successful;
        let global_failed_ops: i32 = self.failed;
        let global_unknown_ops: i32 = self.unknown;
        println!("participant_{}:\tC:{}\tA:{}\tU:{}", self.id, global_successful_ops, global_failed_ops, global_unknown_ops);
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("participant_{} waiting for exit signal", self.id);

        let mut val = self.running.load(Ordering::SeqCst);
        while val {
            val = self.running.load(Ordering::SeqCst);
        }

        trace!("participant_{} exiting", self.id);
    }    

    ///
    /// protocol()
    /// Implements the participant side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep handling requests!
    /// HINT: wait for some kind of exit signal before returning from the protocol!
    /// 
    pub fn protocol(&mut self) {
        
        trace!("Participant_{}::protocol", self.id);

        let mut running;
        loop {
            running = self.running.load(Ordering::SeqCst);
            if running {
                let res = self.ports.1.recv();
                match res {
                    Ok(pm) => {
                        let rf: Option<ProtocolMessage> = Some(pm);
                        let _res = self.perform_operation(&rf);
                    },
                    Err(_err) => break,
                }
                // report stats here based on res
            } else {
                break;
            }
        }

        self.wait_for_exit_signal();
        self.report_status();
    }
}
