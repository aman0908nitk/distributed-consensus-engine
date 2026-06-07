import threading
import time

NUM_NODES = 5


# =========================
# NODE & LEADER ELECTION
# =========================

class Node:
    def __init__(self, node_id):
        self.node_id = node_id
        self.is_leader = False
        self.alive = True
        self.last_heartbeat = time.time()


nodes = [Node(i) for i in range(1, NUM_NODES + 1)]

leader = max(nodes, key=lambda n: n.node_id)
leader.is_leader = True


def print_cluster_status():
    print("\n=== Cluster Status ===")
    for node in nodes:
        role = "Leader" if node.is_leader else "Follower"
        state = "Alive" if node.alive else "Failed"
        print(f"Node {node.node_id} -> {role} ({state})")


def send_heartbeats():
    global leader

    while leader and leader.alive:
        print(f"[Heartbeat] Leader Node {leader.node_id}")

        for node in nodes:
            if node.node_id != leader.node_id:
                node.last_heartbeat = time.time()

        time.sleep(2)


def elect_new_leader():
    global leader

    candidates = [
        n for n in nodes
        if n.alive and n.node_id != leader.node_id
    ]

    if not candidates:
        print("No alive nodes available.")
        return

    for n in nodes:
        n.is_leader = False

    leader = max(candidates, key=lambda n: n.node_id)
    leader.is_leader = True

    print(
        f"\n[ELECTION] New Leader Elected -> Node {leader.node_id}"
    )

    threading.Thread(
        target=send_heartbeats,
        daemon=True
    ).start()


# =========================
# PAXOS
# =========================

def run_paxos(transaction):
    global leader

    print("\n==============================")
    print("       PAXOS PROTOCOL")
    print("==============================")

    print("\nClient Sending Transaction...")
    print(
        f"Leader Node {leader.node_id} received transaction: "
        f"{transaction}"
    )

    print("\nPHASE 1 : PREPARE\n")

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Leader -> Node {node.node_id} : PREPARE"
            )

    print("\nPHASE 2 : PROMISE\n")

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Node {node.node_id} -> PROMISE"
            )

    print("\nPHASE 3 : ACCEPT\n")

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Leader -> Node {node.node_id} "
                f": ACCEPT ({transaction})"
            )

    print("\nPHASE 4 : ACCEPTED\n")

    accepted = 0

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Node {node.node_id} -> ACCEPTED"
            )
            accepted += 1

    if accepted >= (NUM_NODES // 2):
        print("\nPAXOS CONSENSUS REACHED")
        print("Transaction Committed")
    else:
        print("\nPAXOS CONSENSUS FAILED")


# =========================
# PBFT
# =========================

def run_pbft(transaction):
    global leader

    print("\n==============================")
    print("        PBFT PROTOCOL")
    print("==============================")

    print("\nClient Sending Transaction...")
    print(
        f"Primary Node {leader.node_id} "
        f"received transaction: {transaction}"
    )

    print("\nPHASE 1 : PRE-PREPARE\n")

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Primary -> Node {node.node_id} "
                f": PRE-PREPARE ({transaction})"
            )

    print("\nPHASE 2 : PREPARE\n")

    prepare_votes = 0

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Node {node.node_id} -> PREPARE"
            )
            prepare_votes += 1

    print(
        f"\nPrepare Votes Received = "
        f"{prepare_votes}"
    )

    print("\nPHASE 3 : COMMIT\n")

    commit_votes = 0

    for node in nodes:
        if node.node_id != leader.node_id and node.alive:
            print(
                f"Node {node.node_id} -> COMMIT"
            )
            commit_votes += 1

    print(
        f"\nCommit Votes Received = "
        f"{commit_votes}"
    )

    if commit_votes >= 4:
        print("\nPBFT CONSENSUS REACHED")
        print("Transaction Committed")
    else:
        print("\nPBFT CONSENSUS FAILED")


# =========================
# MAIN EXECUTION
# =========================

if __name__ == "__main__":

    print("\n=== INITIAL LEADER ELECTION ===")
    print_cluster_status()

    threading.Thread(
        target=send_heartbeats,
        daemon=True
    ).start()

    time.sleep(3)

    run_paxos("TX001 : Pay Rs 100")

    time.sleep(3)

    run_pbft("TX002 : Pay Rs 500")

    time.sleep(3)

    print(
        "\n***** Simulating Leader Crash *****"
    )

    leader.alive = False

    time.sleep(2)

    elect_new_leader()

    time.sleep(3)

    run_paxos("TX003 : Pay Rs 200")

    time.sleep(3)

    run_pbft("TX004 : Pay Rs 700")

    print(
        "\n===== SYSTEM EXECUTION COMPLETED ====="
    )