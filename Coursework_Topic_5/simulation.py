import simpy
import random
import numpy as np

# Configuration
SIMULATION_TIME = 1800  # 30 minutes in seconds

# System 1 Parameters
ARRIVAL_MEAN_1 = 1.0  # Exponential, mean = 1s
PROCESS_MIN_1 = 1.0   # 2 - 1
PROCESS_MAX_1 = 3.0   # 2 + 1

# System 2 Parameters
ARRIVAL_MEAN_2 = 3.0
ARRIVAL_DEV_2 = 1.0   # Max deviation. Assuming 3*sigma = 1 => sigma = 0.33
PROCESS_MIN_2 = 1.0   # 2 - 1
PROCESS_MAX_2 = 3.0   # 2 + 1

class Monitor:
    def __init__(self):
        self.arrivals_1 = []
        self.arrivals_2 = []
        self.processed_1 = []
        self.processed_2 = []
        self.queue_times_1 = []
        self.queue_times_2 = []
        self.busy_time_1 = 0
        self.busy_time_2 = 0

monitor = Monitor()

def process_request(env, name, server, process_min, process_max, monitor_queue, monitor_busy_ref):
    arrival_time = env.now
    
    with server.request() as req:
        yield req
        
        # Request enters service
        wait_time = env.now - arrival_time
        monitor_queue.append(wait_time)
        
        # Processing
        proc_time = random.uniform(process_min, process_max)
        monitor_busy_ref[0] += proc_time
        
        yield env.timeout(proc_time)
        # Done

def source_1(env, server):
    """Source for Computer 1: Exponential Inter-arrival"""
    i = 0
    while True:
        # Inter-arrival
        yield env.timeout(random.expovariate(1.0 / ARRIVAL_MEAN_1))
        i += 1
        monitor.arrivals_1.append(env.now)
        env.process(process_request(env, f'Req1-{i}', server, PROCESS_MIN_1, PROCESS_MAX_1, monitor.queue_times_1, [monitor.busy_time_1]))
        # Note: busy_time update with list ref is a bit hacky for simple vars, 
        # but here we just need to track it. Actually, better to track busy time globally or in object.

def source_2(env, server):
    """Source for Computer 2: Normal Inter-arrival"""
    i = 0
    while True:
        # Inter-arrival
        # Normal distribution can be negative, need to clamp or retry. 
        # With mean 3 and sigma 0.33, negative is very unlikely (9 sigma).
        dt = random.gauss(ARRIVAL_MEAN_2, ARRIVAL_DEV_2 / 3.0)
        if dt < 0: dt = 0
        yield env.timeout(dt)
        i += 1
        monitor.arrivals_2.append(env.now)
        env.process(process_request(env, f'Req2-{i}', server, PROCESS_MIN_2, PROCESS_MAX_2, monitor.queue_times_2, [monitor.busy_time_2]))

class ComputerSystem:
    def __init__(self, env):
        self.server1 = simpy.Resource(env, capacity=1)
        self.server2 = simpy.Resource(env, capacity=1)
        
        # Tracking busy time properly
        self.busy_t1 = 0.0
        self.busy_t2 = 0.0
        
    def process_1(self, env):
        i = 0
        while True:
            yield env.timeout(random.expovariate(1.0 / ARRIVAL_MEAN_1))
            i += 1
            env.process(self.handle_req_1(env))
            
    def handle_req_1(self, env):
        arrival_time = env.now
        with self.server1.request() as req:
            yield req
            wait = env.now - arrival_time
            monitor.queue_times_1.append(wait)
            
            pt = random.uniform(PROCESS_MIN_1, PROCESS_MAX_1)
            self.busy_t1 += pt
            yield env.timeout(pt)
            monitor.processed_1.append(env.now)

    def process_2(self, env):
        i = 0
        while True:
            dt = random.gauss(ARRIVAL_MEAN_2, ARRIVAL_DEV_2 / 3.0)
            if dt < 0: dt = 0
            yield env.timeout(dt)
            i += 1
            env.process(self.handle_req_2(env))

    def handle_req_2(self, env):
        arrival_time = env.now
        with self.server2.request() as req:
            yield req
            wait = env.now - arrival_time
            monitor.queue_times_2.append(wait)
            
            pt = random.uniform(PROCESS_MIN_2, PROCESS_MAX_2)
            self.busy_t2 += pt
            yield env.timeout(pt)
            monitor.processed_2.append(env.now)

def main():
    env = simpy.Environment()
    system = ComputerSystem(env)
    
    env.process(system.process_1(env))
    env.process(system.process_2(env))
    
    env.run(until=SIMULATION_TIME)
    
    print(f"Simulation Time: {SIMULATION_TIME}s")
    print("-" * 30)
    print("Computer 1 (Exp Arrival, Uniform Process):")
    print(f"  Requests Processed: {len(monitor.processed_1)}")
    print(f"  Avg Wait Time: {np.mean(monitor.queue_times_1):.4f} s")
    print(f"  Max Wait Time: {np.max(monitor.queue_times_1):.4f} s")
    print(f"  Utilization: {system.busy_t1 / SIMULATION_TIME * 100:.2f}%")
    
    print("-" * 30)
    print("Computer 2 (Normal Arrival, Uniform Process):")
    print(f"  Requests Processed: {len(monitor.processed_2)}")
    print(f"  Avg Wait Time: {np.mean(monitor.queue_times_2):.4f} s")
    print(f"  Max Wait Time: {np.max(monitor.queue_times_2):.4f} s")
    print(f"  Utilization: {system.busy_t2 / SIMULATION_TIME * 100:.2f}%")
    
    # Generate data for TikZ
    # We will save wait times to a file to be plotted
    with open('wait_times_1.txt', 'w') as f:
        for t in monitor.queue_times_1:
            f.write(f"{t}\n")
            
    with open('wait_times_2.txt', 'w') as f:
        for t in monitor.queue_times_2:
            f.write(f"{t}\n")

if __name__ == "__main__":
    main()
