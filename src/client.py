import socket
import json
import time
import sys

def submit_transaction(tx_val):
    payload = {"type": "TX_SUBMIT", "val": tx_val}
    ports = [8001, 8002, 8003, 8004, 8005]
    
    for port in ports:
        try:
            print(f"[{port}] Attempting submission of transaction: {tx_val}")
            s = socket.create_connection(("toxiproxy", port), timeout=1.0)
            s.sendall((json.dumps(payload) + "\n").encode('utf-8'))
            s.close()
            # Give the consensus engine a brief moment to process before returning
            time.sleep(0.5)
        except Exception:
            # Port might be disabled by Toxiproxy, skip silently
            continue

if __name__ == "__main__":
    if len(sys.argv) > 1:
        submit_transaction(sys.argv[1])
    else:
        # Default scenario pipelines matching chaos execution expectations
        submit_transaction("Tx_A1_LinearizableLedger")
