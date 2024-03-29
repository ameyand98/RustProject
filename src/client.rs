//! 
//! client.rs
//! Implementation of 2PC client
//! 
extern crate log;
extern crate stderrlog;
use std::sync::mpsc::{Sender, Receiver};
use std::sync::atomic::{AtomicI32, AtomicBool, Ordering};
use std::sync::{Arc};
use std::time::Duration;
use std::thread;
use std::collections::HashMap;
use message;
use message::MessageType;
use message::RequestStatus;

// static counter for getting unique TXID numbers
static TXID_COUNTER: AtomicI32 = AtomicI32::new(1);

// client state and 
// primitives for communicating with 
// the coordinator
#[derive(Debug)]
pub struct Client {    
    pub id: i32,
    pub id_str: String,
    pub ports: (Sender<message::ProtocolMessage>, Receiver<message::ProtocolMessage>),
    pub running: Arc<AtomicBool>,
    pub successful: i32,
    pub failed: i32, 
    pub unknown: i32,
    pub opid: i32,
}

///
/// client implementation
/// Required: 
/// 1. new -- ctor
/// 2. pub fn report_status -- reports number of committed/aborted/unknown 
/// 3. pub fn protocol(&mut self, n_requests: i32) -- implements client side protocol
///
impl Client {

    /// 
    /// new()
    /// 
    /// Return a new client, ready to run the 2PC protocol
    /// with the coordinator. 
    /// 
    /// HINT: you may want to pass some channels or other communication 
    ///       objects that enable coordinator->client and client->coordinator
    ///       messaging to this ctor.
    /// HINT: you may want to pass some global flags that indicate whether
    ///       the protocol is still running to this constructor
    /// 
    pub fn new(i: i32,
               is: String,
               tx: Sender<message::ProtocolMessage>,
               rx: Receiver<message::ProtocolMessage>,
               r: Arc<AtomicBool>) -> Client {
        Client {
            id: i,
            id_str: is,
            ports: (tx, rx),
            running: r,
            successful: 0,
            failed: 0,
            unknown: 0,
            opid: 0,
        }   
    }

    ///
    /// wait_for_exit_signal(&mut self)
    /// wait until the running flag is set by the CTRL-C handler
    /// 
    pub fn wait_for_exit_signal(&mut self) {

        trace!("Client_{} waiting for exit signal", self.id);

        let mut val = self.running.load(Ordering::SeqCst);
        while val {
            val = self.running.load(Ordering::SeqCst);
        }

        trace!("Client_{} exiting", self.id);
    }

    /// 
    /// send_next_operation(&mut self)
    /// send the next operation to the coordinator
    /// 
    pub fn send_next_operation(&mut self) {

        trace!("Client_{}::send_next_operation", self.id);

        // create a new request with a unique TXID.         
        let request_no: i32 = self.opid; 
        self.opid = self.opid + 1;
        let txid = TXID_COUNTER.fetch_add(1, Ordering::SeqCst);

        info!("Client {} request({})->txid:{} called", self.id, request_no, txid);
        let pm = message::ProtocolMessage::generate(message::MessageType::ClientRequest, 
                                                    txid, 
                                                    format!("Client_{}", self.id), 
                                                    request_no);

        info!("client {} calling send...", self.id);

        let res = self.ports.0.send(pm);
        match res {
            // NEED TO LOG
            Ok(_val) => info!("client {} succesfully sent request {}", self.id, request_no),
            Err(err) => panic!("client {} send returned an error {:?}", self.id, err),
        }

        trace!("Client_{}::exit send_next_operation", self.id);
    }

    ///
    /// recv_result()
    /// Wait for the coordinator to respond with the result for the 
    /// last issued request. Note that we assume the coordinator does 
    /// not fail in this simulation
    /// 
    pub fn recv_result(&mut self) {

        trace!("Client_{}::recv_result", self.id);

        let res = self.ports.1.recv();
        match res {
            Ok(result) => {
                info!("client {} parsing result", self.id);
                match result.mtype {
                    // NEED TO LOG
                    message::MessageType::ClientResultCommit => self.successful = self.successful + 1,
                    message::MessageType::ClientResultAbort => self.failed = self.failed + 1,
                    message::MessageType::CoordinatorExit => self.running.store(false, Ordering::SeqCst),
                    _ => self.unknown = self.unknown + 1,
                }
            },
            Err(_err) => {},
        }

        trace!("Client_{}::exit recv_result", self.id);
    }

    ///
    /// report_status()
    /// report the abort/commit/unknown status (aggregate) of all 
    /// transaction requests made by this client before exiting. 
    /// 
    pub fn report_status(&mut self) {

        // TODO: collect real stats!
        let _successful_ops: i32 = self.successful;
        let _failed_ops: i32 = self.failed;
        let _unknown_ops: i32 = self.unknown;
        // println!("Client_{}:\tC:{}\tA:{}\tU:{}", self.id, successful_ops, failed_ops, unknown_ops);
    }    

    ///
    /// protocol()
    /// Implements the client side of the 2PC protocol
    /// HINT: if the simulation ends early, don't keep issuing requests!
    /// HINT: if you've issued all your requests, wait for some kind of
    ///       exit signal before returning from the protocol method!
    /// 
    pub fn protocol(&mut self, n_requests: i32) {

        // run the 2PC protocol for each of n_requests

        let mut running;
        for _i in 0..n_requests {
            running = self.running.load(Ordering::SeqCst);
            if running {
                self.send_next_operation();
                self.recv_result();
            } else {
                break;
            }
        }
        drop(&self.ports.0);

        // wait for signal to exit
        // and then report status
        self.wait_for_exit_signal();
        self.report_status();
    }
}
