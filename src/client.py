import asyncio
import json
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s [CLIENT] %(message)s")

class ConsensusClient:
    def __init__(self, target_nodes: list):
        self.target_nodes = target_nodes     

    async def submit_transaction(self, tx: str, mode: str):
        mode_payload = {
            "type": "MODE_CHANGE",
            "mode": mode
        }                
        for node_addr in self.target_nodes:
            try:
                ip, port = node_addr.split(":")
                reader, writer = await asyncio.open_connection(ip, int(port))
                writer.write((json.dumps(mode_payload) + "\n").encode())                 
                await writer.drain()
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass        
        success = False        
        for node_addr in self.target_nodes:
            try:
                ip, port = node_addr.split(":")
                logging.info(f"Connecting to node at {node_addr} to submit transaction: {tx}")
                reader, writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, int(port)), timeout=1.0
                )                                
                proposal_payload = {
                    "type": "TX_SUBMIT",
                    "val": tx                
                }                                
                writer.write((json.dumps(proposal_payload) + "\n").encode())
                await writer.drain()
                writer.close()
                await writer.wait_closed()                
                success = True
                break
            except Exception as e:
                logging.warning(f"Failed to connect to node {node_addr}: {e}. Trying next...")                        
        if success:
            logging.info("Transaction proposal submitted successfully.")
        else:
            logging.error("Failed to submit transaction: quorum target unreachable.")

async def main():
    nodes = ["toxiproxy:8001", "toxiproxy:8002", "toxiproxy:8003", "toxiproxy:8004", "toxiproxy:8005"]
    client = ConsensusClient(nodes)        
    await client.submit_transaction("Tx_A1_LinearizableLedger", "A")
    await asyncio.sleep(2)
    await client.submit_transaction("Tx_B1_ByzantineResistantProof", "B")

if __name__ == "__main__":
    asyncio.run(main())
