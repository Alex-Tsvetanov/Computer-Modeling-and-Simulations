"""
Topic 28: Distributed Database System
Complex distributed system with 3 remote centers (A, B, C)
- Duplex communication channels
- Parallel queries to all 3 centers
- Response collection from all centers required
"""

import simpy
import random
import numpy as np

# Constants
NUM_REQUESTS = 150
ARRIVAL_MEAN = 12
ARRIVAL_VAR = 5

# Processing times
PREPROCESS_MIN = 1  # 2 +/- 1
PREPROCESS_MAX = 3

# Channel transmission times
OUTGOING_CHANNEL_TIME = 1  # To centers
INCOMING_CHANNEL_TIME = 2   # From centers

# Search times at each center
CENTER_A_MIN = 3   # 5 +/- 2
CENTER_A_MAX = 7

CENTER_B_MIN = 5   # 10 +/- 5
CENTER_B_MAX = 15

CENTER_C_MIN = 4   # 8 +/- 4
CENTER_C_MAX = 12


class DistributedDatabaseSystem:
    def __init__(self, env):
        self.env = env
        
        # Central computer for preprocessing
        self.central_computer = simpy.Resource(env, capacity=1)
        
        # Three remote centers
        self.center_a = simpy.Resource(env, capacity=1)
        self.center_b = simpy.Resource(env, capacity=1)
        self.center_c = simpy.Resource(env, capacity=1)
        
        # Communication channels (duplex)
        self.outgoing_channel = simpy.Resource(env, capacity=1)
        self.incoming_channel = simpy.Resource(env, capacity=1)
        
        # Statistics
        self.requests_generated = 0
        self.requests_completed = 0
        
        # Timing statistics
        self.total_times = []
        self.preprocess_times = []
        self.query_times_a = []
        self.query_times_b = []
        self.query_times_c = []
        self.wait_times_center_a = []
        self.wait_times_center_b = []
        self.wait_times_center_c = []
    
    def request_generator(self):
        """Generate requests every (12 +/- 5) minutes"""
        while self.requests_generated < NUM_REQUESTS:
            interarrival = random.uniform(ARRIVAL_MEAN - ARRIVAL_VAR,
                                         ARRIVAL_MEAN + ARRIVAL_VAR)
            yield self.env.timeout(interarrival)
            
            self.requests_generated += 1
            request_id = self.requests_generated
            arrival_time = self.env.now
            
            print(f"[{self.env.now:.2f}] Request {request_id} arrived")
            
            # Start processing the request
            self.env.process(self.process_request(request_id, arrival_time))
    
    def process_request(self, request_id, arrival_time):
        """Process a request through the distributed system"""
        # Step 1: Preprocessing at central computer
        preprocess_start = self.env.now
        
        with self.central_computer.request() as req:
            yield req
            preprocess_time = random.uniform(PREPROCESS_MIN, PREPROCESS_MAX)
            yield self.env.timeout(preprocess_time)
            self.preprocess_times.append(preprocess_time)
        
        print(f"[{self.env.now:.2f}] Request {request_id} preprocessing complete "
              f"(took {preprocess_time:.2f} min)")
        
        # Step 2: Send queries to all 3 centers in parallel via outgoing channels
        # Each query transmission takes 1 minute
        query_start = self.env.now
        
        # Send queries to centers (can be parallel since different channels conceptually)
        query_a = self.env.process(self.query_center_a(request_id))
        query_b = self.env.process(self.query_center_b(request_id))
        query_c = self.env.process(self.query_center_c(request_id))
        
        # Wait for all queries to complete
        results = yield simpy.AllOf(self.env, [query_a, query_b, query_c])
        
        query_time = self.env.now - query_start
        print(f"[{self.env.now:.2f}] Request {request_id} all center responses received "
              f"(query phase took {query_time:.2f} min)")
        
        # Request is complete
        total_time = self.env.now - arrival_time
        self.total_times.append(total_time)
        self.requests_completed += 1
        
        print(f"[{self.env.now:.2f}] Request {request_id} COMPLETED "
              f"(total time: {total_time:.2f} min)")
    
    def query_center_a(self, request_id):
        """Query center A: transmit (1 min), search (5±2 min), receive (2 min)"""
        # Transmit request to center A
        with self.outgoing_channel.request() as req:
            yield req
            yield self.env.timeout(OUTGOING_CHANNEL_TIME)
        
        # Search at center A
        queue_start = self.env.now
        with self.center_a.request() as req:
            yield req
            wait_time = self.env.now - queue_start
            self.wait_times_center_a.append(wait_time)
            
            search_time = random.uniform(CENTER_A_MIN, CENTER_A_MAX)
            yield self.env.timeout(search_time)
            self.query_times_a.append(search_time)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center A search complete")
        
        # Receive response from center A
        with self.incoming_channel.request() as req:
            yield req
            yield self.env.timeout(INCOMING_CHANNEL_TIME)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center A response received")
    
    def query_center_b(self, request_id):
        """Query center B: transmit (1 min), search (10±5 min), receive (2 min)"""
        # Transmit request to center B
        with self.outgoing_channel.request() as req:
            yield req
            yield self.env.timeout(OUTGOING_CHANNEL_TIME)
        
        # Search at center B
        queue_start = self.env.now
        with self.center_b.request() as req:
            yield req
            wait_time = self.env.now - queue_start
            self.wait_times_center_b.append(wait_time)
            
            search_time = random.uniform(CENTER_B_MIN, CENTER_B_MAX)
            yield self.env.timeout(search_time)
            self.query_times_b.append(search_time)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center B search complete")
        
        # Receive response from center B
        with self.incoming_channel.request() as req:
            yield req
            yield self.env.timeout(INCOMING_CHANNEL_TIME)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center B response received")
    
    def query_center_c(self, request_id):
        """Query center C: transmit (1 min), search (8±4 min), receive (2 min)"""
        # Transmit request to center C
        with self.outgoing_channel.request() as req:
            yield req
            yield self.env.timeout(OUTGOING_CHANNEL_TIME)
        
        # Search at center C
        queue_start = self.env.now
        with self.center_c.request() as req:
            yield req
            wait_time = self.env.now - queue_start
            self.wait_times_center_c.append(wait_time)
            
            search_time = random.uniform(CENTER_C_MIN, CENTER_C_MAX)
            yield self.env.timeout(search_time)
            self.query_times_c.append(search_time)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center C search complete")
        
        # Receive response from center C
        with self.incoming_channel.request() as req:
            yield req
            yield self.env.timeout(INCOMING_CHANNEL_TIME)
        
        print(f"[{self.env.now:.2f}] Request {request_id} Center C response received")


def run_simulation():
    """Run the distributed database system simulation"""
    print("=" * 70)
    print("TOPIC 28: DISTRIBUTED DATABASE SYSTEM SIMULATION")
    print("=" * 70)
    print(f"Number of requests: {NUM_REQUESTS}")
    print(f"Arrival interval: ({ARRIVAL_MEAN} +/- {ARRIVAL_VAR}) minutes")
    print(f"Preprocessing: ({2} +/- {1}) minutes")
    print(f"Outgoing channel: {OUTGOING_CHANNEL_TIME} min, Incoming: {INCOMING_CHANNEL_TIME} min")
    print(f"Center A search: ({5} +/- {2}) min")
    print(f"Center B search: ({10} +/- {5}) min")
    print(f"Center C search: ({8} +/- {4}) min")
    print("=" * 70)
    
    env = simpy.Environment()
    system = DistributedDatabaseSystem(env)
    
    # Start request generation
    env.process(system.request_generator())
    
    # Run until all requests are completed
    while system.requests_completed < NUM_REQUESTS:
        env.step()
    
    # Print results
    print("\n" + "=" * 70)
    print("SIMULATION RESULTS")
    print("=" * 70)
    
    print(f"\nRequest Statistics:")
    print(f"  Requests generated: {system.requests_generated}")
    print(f"  Requests completed: {system.requests_completed}")
    
    print(f"\nTotal Time Statistics:")
    if system.total_times:
        print(f"  Average: {np.mean(system.total_times):.2f} min")
        print(f"  Minimum: {np.min(system.total_times):.2f} min")
        print(f"  Maximum: {np.max(system.total_times):.2f} min")
    
    print(f"\nPreprocessing Time:")
    if system.preprocess_times:
        print(f"  Average: {np.mean(system.preprocess_times):.2f} min")
    
    print(f"\nCenter A (fastest, 5±2 min):")
    if system.query_times_a:
        print(f"  Queries: {len(system.query_times_a)}")
        print(f"  Avg search time: {np.mean(system.query_times_a):.2f} min")
        print(f"  Avg wait time: {np.mean(system.wait_times_center_a):.2f} min")
    
    print(f"\nCenter B (slowest, 10±5 min):")
    if system.query_times_b:
        print(f"  Queries: {len(system.query_times_b)}")
        print(f"  Avg search time: {np.mean(system.query_times_b):.2f} min")
        print(f"  Avg wait time: {np.mean(system.wait_times_center_b):.2f} min")
    
    print(f"\nCenter C (medium, 8±4 min):")
    if system.query_times_c:
        print(f"  Queries: {len(system.query_times_c)}")
        print(f"  Avg search time: {np.mean(system.query_times_c):.2f} min")
        print(f"  Avg wait time: {np.mean(system.wait_times_center_c):.2f} min")
    
    # Theoretical calculation
    # Total time = preprocess + max(A, B, C) + transmission times
    # preprocess: 2 min avg
    # A: 1 + 5 + 2 = 8 min
    # B: 1 + 10 + 2 = 13 min (bottleneck)
    # C: 1 + 8 + 2 = 11 min
    print(f"\nTheoretical Analysis:")
    print(f"  Preprocessing: ~2 min")
    print(f"  Center A total: 1 + 5 + 2 = 8 min")
    print(f"  Center B total: 1 + 10 + 2 = 13 min (bottleneck)")
    print(f"  Center C total: 1 + 8 + 2 = 11 min")
    print(f"  Expected total: ~2 + 13 = 15 min (limited by slowest center)")
    
    # Save results
    with open('simulation_results.txt', 'w', encoding='utf-8') as f:
        f.write("Topic 28 Distributed Database System Results\n")
        f.write("=" * 50 + "\n")
        f.write(f"Requests generated: {system.requests_generated}\n")
        f.write(f"Requests completed: {system.requests_completed}\n")
        if system.total_times:
            f.write(f"Avg total time: {np.mean(system.total_times):.2f} min\n")
            f.write(f"Min total time: {np.min(system.total_times):.2f} min\n")
            f.write(f"Max total time: {np.max(system.total_times):.2f} min\n")
    
    return system


if __name__ == "__main__":
    random.seed(42)
    run_simulation()
