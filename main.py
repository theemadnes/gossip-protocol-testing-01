import random
import time
import threading

class Node:
    def __init__(self, node_id, num_nodes):
        self.id = node_id
        self.num_nodes = num_nodes
        self.data = {}  # Store key-value pairs with vector clocks
        self.neighbors = []
        self.lock = threading.Lock()

    def add_neighbor(self, neighbor):
        self.neighbors.append(neighbor)

    def disseminate(self, key, value):
        with self.lock:
            if key not in self.data:
                self.data[key] = {"value": value, "vc": [0] * self.num_nodes}
            self.data[key]["vc"][self.id] += 1  # Increment local clock
        self.gossip(key)

    def gossip(self, key):
        if not self.neighbors:
            return

        num_to_gossip = min(3, len(self.neighbors))
        gossiped_to = random.sample(self.neighbors, num_to_gossip)

        for neighbor in gossiped_to:
            neighbor.receive(self.id, key, self.data[key])

    def receive(self, sender_id, key, received_data):
        with self.lock:
            if key not in self.data:
                self.data[key] = {"value": None, "vc": [0] * self.num_nodes}

            current_vc = self.data[key]["vc"]
            received_vc = received_data["vc"]

            if self.should_update(current_vc, received_vc):
                self.data[key]["value"] = received_data["value"]
                #Merge vector clocks
                for i in range(self.num_nodes):
                    current_vc[i] = max(current_vc[i], received_vc[i])

                print(f"Node {self.id} received update for {key}: {self.data[key]} from Node {sender_id}")
                self.gossip(key)

    def should_update(self, current_vc, received_vc):
        """Determines if the received data is newer based on vector clocks."""
        current_is_older = False
        received_is_older = False
        for i in range(self.num_nodes):
            if current_vc[i] < received_vc[i]:
                current_is_older = True
            if received_vc[i] < current_vc[i]:
                received_is_older = True
        return current_is_older or not received_is_older # Update if current is older or neither is older (concurrent)

    def print_data(self):
        with self.lock:
            print(f"Node {self.id} data: {self.data}")


def simulate_gossip(nodes, initial_data):
    for node_id, (key, value) in initial_data.items():
        nodes[node_id].disseminate(key, value)

    time.sleep(5)

    for node in nodes:
        node.print_data()

if __name__ == "__main__":
    num_nodes = 5
    nodes = [Node(i, num_nodes) for i in range(num_nodes)]

    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            nodes[i].add_neighbor(nodes[j])
            nodes[j].add_neighbor(nodes[i])

    initial_data = {
        0: ("temperature", 25),
        1: ("humidity", 60),
        2: ("pressure", 1013),
        0:("temperature", 27), #Update a value
        1:("humidity", 65)
    }

    simulate_gossip(nodes, initial_data)