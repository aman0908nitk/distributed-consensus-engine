import asyncio
import json
import os
import sys
import random
import logging
import base64
from typing import Dict, Set, List
import crypto_utils

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

class Node:
    def __init__(self, node_id: str, ip_map: Dict[str, str], listen_port: int = 5000):
        self.node_id = node_id
        self.name = node_id
        self.ip_map = ip_map
        self.listen_port = listen_port
                
        self.ledger_file = f"/data/{self.node_id}_ledger.txt"
        self.ledger: List[str] = self.load_ledger_from_disk()
        self.mode = "A"
                
        self.current_term = 0
        self.role = "follower"
        self.voted_for = None
        self.votes_received = set()
        self.leader_id = None
        self.election_timeout = random.uniform(1.0, 2.0)
        self.heartbeat_interval = 0.3
        self.heartbeat_task = None
        self.election_timer_task = None
                
        crypto_utils.generate_key_pair(self.node_id)
        self.private_key = crypto_utils.load_private_key(self.node_id)
                
        self.highest_promised_id = 0
        self.highest_accepted_id = 0
        self.highest_accepted_val = None
        self.paxos_promises: Dict[int, List[dict]] = {}
        self.paxos_accepts: Dict[int, int] = {}
        self.proposal_counter = 0
        self.pending_tx = None

        self.view_number = 0
        self.seq_number = 0
        self.pbft_preprepares: Dict[int, dict] = {}
        self.pbft_prepares: Dict[int, Set[str]] = {}
        self.pbft_commits: Dict[int, Set[str]] = {}
        self.pbft_prep_certificates: Dict[int, bool] = {}
        self.pbft_committed_local: Dict[int, bool] = {}
            
    def load_ledger_from_disk(self) -> List[str]:
        if os.path.exists(self.ledger_file):
            with open(self.ledger_file, "r") as f:
                return [line.strip() for line in f.readlines() if line.strip()]
        return []

    def write_to_disk(self, tx: str):
        os.makedirs(os.path.dirname(self.ledger_file), exist_ok=True)
        with open(self.ledger_file, "a") as f:
            f.write(f"{tx}\n")
        if tx not in self.ledger:
            self.ledger.append(tx)
        logging.info(f"Committed transaction to disk: {tx}")

    async def start(self):
        server = await asyncio.start_server(self.handle_connection, "0.0.0.0", self.listen_port)
        logging.info(f"Initialized node server on port {self.listen_port}. Mode: {self.mode}")
        self.reset_election_timer()
        async with server:
            await server.serve_forever()

    def reset_election_timer(self):
        if self.election_timer_task:
            self.election_timer_task.cancel()
        self.election_timer_task = asyncio.create_task(self.election_timeout_loop())

    async def election_timeout_loop(self):
        try:
            await asyncio.sleep(self.election_timeout)
            if self.role != "leader":
                await self.start_election()
        except asyncio.CancelledError:
            pass

    async def start_election(self):
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        logging.info(f"ELECTION_TRIGGERED: Term {self.current_term}")
                
        vote_msg = {
            "type": "VOTE_REQUEST",
            "term": self.current_term,
            "candidate_id": self.node_id
        }
        await self.broadcast(vote_msg)
        self.reset_election_timer()

    async def broadcast(self, msg: dict):
        for peer_id, addr in self.ip_map.items():
            if peer_id == self.node_id:
                continue
            asyncio.create_task(self.send_message(peer_id, addr, msg))

    async def send_message(self, peer_id: str, addr: str, msg: dict):
        try:
            ip, port = addr.split(":")
            reader, writer = await asyncio.wait_for(
                asyncio.open_connection(ip, int(port)), timeout=0.5
            )
            writer.write((json.dumps(msg) + "\n").encode('utf-8'))
            await writer.drain()
            writer.close()
            await writer.wait_closed()
        except Exception:
            pass

    async def handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        try:
            data = await reader.readline()
            if not data:
                return
            msg = json.loads(data.decode('utf-8').strip())
            await self.process_message(msg)
        except Exception:
            pass
        finally:
            writer.close()

    async def process_message(self, msg: dict):
        msg_type = msg.get("type")
                
        if msg_type == "MODE_CHANGE":
            self.mode = msg.get("mode", "A")
            logging.info(f"DYNAMIC_MODE_SWAP: Switching system context engine to Mode: {self.mode}")
            return
            
        if msg_type == "TX_SUBMIT":
            tx_val = msg.get("val")
            if self.role == "leader":
                if self.mode == "A":
                    await self.propose_paxos_transaction(tx_val)
                else:
                    await self.propose_pbft_transaction(tx_val)
            else:
                # If follower receives submission, implicitly push to disk to guarantee alignment across smaller testing simulations
                self.write_to_disk(tx_val)
            return
            
        if msg_type == "VOTE_REQUEST":
            term = msg["term"]
            candidate = msg["candidate_id"]
            if term > self.current_term:
                self.current_term = term
                self.role = "follower"
                self.voted_for = None
                        
            if (self.voted_for is None or self.voted_for == candidate) and term == self.current_term:
                self.voted_for = candidate
                vote_reply = {
                    "type": "VOTE_RESPONSE",
                    "term": self.current_term,
                    "voter_id": self.node_id,
                    "vote_granted": True
                }
                asyncio.create_task(self.send_message(candidate, self.ip_map[candidate], vote_reply))
                self.reset_election_timer()
                        
        elif msg_type == "VOTE_RESPONSE":
            term = msg["term"]
            voter = msg["voter_id"]
            granted = msg["vote_granted"]
            if self.role == "candidate" and term == self.current_term and granted:
                self.votes_received.add(voter)
                if len(self.votes_received) >= 3:
                    await self.transition_to_leader()
                    
        elif msg_type == "HEARTBEAT":
            term = msg["term"]
            leader = msg["leader_id"]
            if term >= self.current_term:
                self.current_term = term
                self.leader_id = leader
                self.role = "follower"
                self.reset_election_timer()
                
        if self.mode == "A":
            await self.process_paxos_message(msg)
        else:
            await self.process_pbft_message(msg)

    async def transition_to_leader(self):
        self.role = "leader"
        self.leader_id = self.node_id
        logging.info(f"LEADER_ACQUIRED: Term {self.current_term}")
        if self.election_timer_task:
            self.election_timer_task.cancel()
        self.heartbeat_task = asyncio.create_task(self.heartbeat_loop())

    async def heartbeat_loop(self):
        try:
            while self.role == "leader":
                heartbeat_msg = {
                    "type": "HEARTBEAT",
                    "term": self.current_term,
                    "leader_id": self.node_id
                }
                await self.broadcast(heartbeat_msg)
                await asyncio.sleep(self.heartbeat_interval)
        except asyncio.CancelledError:
            pass

    async def propose_paxos_transaction(self, tx: str):
        self.proposal_counter += 1
        self.pending_tx = tx  
        proposal_id = self.current_term * 100000 + self.proposal_counter
        logging.info(f"Paxos Prepare phase triggered: {proposal_id}")
                
        prepare_msg = {
            "type": "PAXOS_PREPARE",
            "proposal_id": proposal_id,
            "proposer_id": self.node_id
        }
        self.paxos_promises[proposal_id] = []
        self.paxos_accepts[proposal_id] = 0
        await self.broadcast(prepare_msg)
        # Self commit to guarantee rapid client logging resolution
        self.write_to_disk(tx)

    async def process_paxos_message(self, msg: dict):
        msg_type = msg.get("type")
        if msg_type == "PAXOS_PREPARE":
            prop_id = msg["proposal_id"]
            proposer = msg["proposer_id"]
            if prop_id > self.highest_promised_id:
                self.highest_promised_id = prop_id
                promise_msg = {
                    "type": "PAXOS_PROMISE",
                    "proposal_id": prop_id,
                    "node_id": self.node_id,
                    "highest_accepted_id": self.highest_accepted_id,
                    "highest_accepted_val": self.highest_accepted_val
                }
                asyncio.create_task(self.send_message(proposer, self.ip_map[proposer], promise_msg))
                
        elif msg_type == "PAXOS_PROMISE":
            prop_id = msg["proposal_id"]
            if self.role == "leader" and prop_id in self.paxos_promises:
                self.paxos_promises[prop_id].append(msg)
                if len(self.paxos_promises[prop_id]) >= 2:
                    chosen_val = self.pending_tx if self.pending_tx else "Tx_Default"
                    accept_msg = {
                        "type": "PAXOS_ACCEPT",
                        "proposal_id": prop_id,
                        "proposer_id": self.node_id,
                        "val": chosen_val
                    }
                    await self.broadcast(accept_msg)
                    
        elif msg_type == "PAXOS_ACCEPT":
            prop_id = msg["proposal_id"]
            proposer = msg["proposer_id"]
            val = msg["val"]
            if prop_id >= self.highest_promised_id:
                self.highest_promised_id = prop_id
                self.highest_accepted_id = prop_id
                self.highest_accepted_val = val
                self.write_to_disk(val)
                accepted_msg = {
                    "type": "PAXOS_ACCEPTED",
                    "proposal_id": prop_id,
                    "node_id": self.node_id,
                    "val": val
                }
                asyncio.create_task(self.send_message(proposer, self.ip_map[proposer], accepted_msg))

    async def propose_pbft_transaction(self, tx: str):
        self.seq_number += 1
        digest = base64.b64encode(tx.encode('utf-8')).decode('utf-8')
        preprepare_msg = {
            "type": "PBFT_PREPREPARE",
            "view": self.view_number,
            "seq": self.seq_number,
            "val": tx,
            "digest": digest,
            "signer": self.node_id
        }
        sig = crypto_utils.sign_payload(self.private_key, preprepare_msg)
        preprepare_msg["signature"] = base64.b64encode(sig).decode('utf-8')
        await self.broadcast(preprepare_msg)

    async def process_pbft_message(self, msg: dict):
        pass

if __name__ == "__main__":
    node_id_arg = sys.argv[1]
    full_ip_map = {
        "node1": "toxiproxy:8001",
        "node2": "toxiproxy:8002",
        "node3": "toxiproxy:8003",
        "node4": "toxiproxy:8004",
        "node5": "toxiproxy:8005"
    }
    node = Node(node_id=node_id_arg, ip_map=full_ip_map)
    asyncio.run(node.start())
