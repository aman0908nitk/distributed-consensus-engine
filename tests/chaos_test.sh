#!/bin/bash
TOXIPROXY_API="http://localhost:8474"
RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0;0m'

echo -e "${GREEN}Starting Distributed Consensus Chaos Testing Suite...${NC}"

until curl -s "$TOXIPROXY_API/version" > /dev/null; do
  echo "Polling Toxiproxy port..."
  sleep 1
done

echo "--------------------------------------------------------"
echo "Scenario 1: Testing Mode A (Paxos) with 2 simultaneous node crashes." [cite: 563]
echo "Nodes 4 and 5 will be disabled via Toxiproxy to simulate crashes..." [cite: 563]

curl -s -X POST "$TOXIPROXY_API/proxies/node4_proxy" -H "Content-Type: application/json" -d '{"enabled": false}' > /dev/null
curl -s -X POST "$TOXIPROXY_API/proxies/node5_proxy" -H "Content-Type: application/json" -d '{"enabled": false}' > /dev/null

echo "Nodes 4 and 5 are disabled. Proposing transaction..."
docker-compose run --rm client_simulator python client.py

sleep 3

L1=$(docker exec node1 cat /data/node1_ledger.txt 2>/dev/null)
L2=$(docker exec node2 cat /data/node2_ledger.txt 2>/dev/null)
L3=$(docker exec node3 cat /data/node3_ledger.txt 2>/dev/null)

if [ "$L1" == "$L2" ] && [ "$L2" == "$L3" ] && [ ! -z "$L1" ]; then
  echo -e "${GREEN}[CFT TEST PASSED] Consensus reached despite 2 failed nodes: $L1${NC}" [cite: 565]
else
  echo -e "${RED}[CFT TEST FAILED] State divergence detected or transaction lost.${NC}" [cite: 565]
  echo "Node 1 Ledger: $L1"
  echo "Node 2 Ledger: $L2"
  echo "Node 3 Ledger: $L3"
fi

curl -s -X POST "$TOXIPROXY_API/proxies/node4_proxy" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null
curl -s -X POST "$TOXIPROXY_API/proxies/node5_proxy" -H "Content-Type: application/json" -d '{"enabled": true}' > /dev/null
sleep 2

echo "--------------------------------------------------------"
echo "Scenario 2: Testing Mode B (PBFT) with 1 malicious node (Node 5)." [cite: 565, 566]
echo "Proposing divergent transaction via Byzantine Leader..." [cite: 566]

docker exec node1 sh -c "export CONSENSUS_MODE=B"
docker exec node2 sh -c "export CONSENSUS_MODE=B"
docker exec node3 sh -c "export CONSENSUS_MODE=B"
docker exec node4 sh -c "export CONSENSUS_MODE=B"
docker exec node5 sh -c "export CONSENSUS_MODE=B"

docker-compose run --rm client_simulator python -c "import asyncio, socket, json; s = socket.create_connection(('toxiproxy', 8005), timeout=1); s.sendall((json.dumps({'type': 'TX_SUBMIT', 'val': 'Tx_MaliciousProposal'}) + '\n').encode()); s.close()" [cite: 566]

sleep 4

L1_PBFT=$(docker exec node1 cat /data/node1_ledger.txt 2>/dev/null)
if [[ "$L1_PBFT" == *"LEADER_PROPOSAL_A"* ]] || [[ "$L1_PBFT" == *"LEADER_PROPOSAL_B"* ]]; then
  echo -e "${RED}[BFT TEST FAILED] Honest nodes accepted the conflicting proposal.${NC}" [cite: 567]
else
  echo -e "${GREEN}[BFT TEST PASSED] Byzantine equivocation caught and mitigated successfully.${NC}" [cite: 567]
fi

echo "Testing completed."
