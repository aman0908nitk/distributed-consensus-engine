import asyncio
import base64
import json
import logging
from node import Node
import crypto_utils

class ByzantineAdversary(Node):
    def __init__(self, node_id: str, ip_map: dict):
        super().__init__(node_id, ip_map)
        logging.info("Initialized as MALICIOUS BYZANTINE ADVERSARY NODE")    

    async def propose_pbft_transaction(self, tx: str):
        self.seq_number += 1
        logging.info(f"[Adversary] Executing Equivocation on transaction proposal {self.seq_number}")                 
        tx1 = f"{tx}_LEADER_PROPOSAL_A"
        tx2 = f"{tx}_LEADER_PROPOSAL_B"                
        digest1 = base64.b64encode(tx1.encode()).decode()
        digest2 = base64.b64encode(tx2.encode()).decode()
        msg1 = {
            "type": "PBFT_PREPREPARE",            
            "view": self.view_number,
            "seq": self.seq_number,
            "val": tx1,
            "digest": digest1,
            "signer": self.node_id
        }
        sig1 = crypto_utils.sign_payload(self.private_key, msg1)
        msg1["signature"] = base64.b64encode(sig1).decode()
        msg2 = {            
            "type": "PBFT_PREPREPARE",
            "view": self.view_number,
            "seq": self.seq_number,
            "val": tx2,
            "digest": digest2,
            "signer": self.node_id
        }
        sig2 = crypto_utils.sign_payload(self.private_key, msg2)
        msg2["signature"] = base64.b64encode(sig2).decode()        
        for idx, (peer_id, addr) in enumerate(self.ip_map.items()):
            if peer_id == self.node_id:
                continue
            if idx <= 2:
                logging.info(f"[Adversary] Sending conflicting payload A to {peer_id}")    
                asyncio.create_task(self.send_message(peer_id, addr, msg1))
            else:
                logging.info(f"[Adversary] Sending conflicting payload B to {peer_id}")
                asyncio.create_task(self.send_message(peer_id, addr, msg2))

    async def process_pbft_message(self, msg: dict):
        msg_type = msg.get("type")    
        if msg_type == "PBFT_PREPARE":
            logging.info("[Adversary] Intercepted message: Suppressing commit message broadcast.")
            return                 
        await super().process_pbft_message(msg)

if __name__ == "__main__":
    import sys
    node_id_arg = sys.argv[1]        
    full_ip_map = {         
        "node1": "toxiproxy:8001",
        "node2": "toxiproxy:8002",
        "node3": "toxiproxy:8003",
        "node4": "toxiproxy:8004",
        "node5": "toxiproxy:8005"
    }        
    adversary = ByzantineAdversary(node_id=node_id_arg, ip_map=full_ip_map)
    asyncio.run(adversary.start())
