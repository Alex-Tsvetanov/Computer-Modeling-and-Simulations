"""
Topic 9: Data Collection System with Sensor Grouping and Retry Logic
Complex system with:
- Two sensor types (A and B) with different arrival patterns
- Grouping 4 messages into tasks
- Preprocessing by 2 processors with 24% failure retry
- Distribution module collecting 2A + 3B messages
- Dual storage (main and backup)
"""

import simpy
import random
import numpy as np

# Constants
SIMULATION_HOURS = 4
SIMULATION_TIME = SIMULATION_HOURS * 3600  # 4 hours in seconds

# Sensor A: arrives every (9 ± 4) seconds
SENSOR_A_MEAN = 9
SENSOR_A_VAR = 4

# Sensor B: arrives every 2 seconds
SENSOR_B_INTERVAL = 2

# Preprocessing
PREPROCESS_TIME = 10
PREPROCESS_FAILURE_RATE = 0.24  # 24% failure

# Distribution
MESSAGES_PER_TASK = 4  # 4 messages per task for preprocessing
A_FOR_DISTRIBUTION = 2  # Need 2 type A messages
B_FOR_DISTRIBUTION = 3  # Need 3 type B messages

# Storage time
STORAGE_TIME = 0.5


class DataCollectionSystem:
    def __init__(self, env):
        self.env = env
        
        # Resources
        self.processor1 = simpy.Resource(env, capacity=1)
        self.processor2 = simpy.Resource(env, capacity=1)
        self.main_storage = simpy.Resource(env, capacity=1)
        self.backup_storage = simpy.Resource(env, capacity=1)
        
        # Statistics
        self.sensor_a_generated = 0
        self.sensor_b_generated = 0
        self.messages_stored = 0
        self.tasks_preprocessed = 0
        self.tasks_failed = 0
        self.tasks_retried = 0
        self.distributions_completed = 0
        
        # Buffers for grouping and distribution
        self.task_buffer = []  # For grouping 4 messages into tasks
        self.preprocessed_a = []  # Successfully preprocessed A messages
        self.preprocessed_b = []  # Successfully preprocessed B messages
        
        # Timing statistics
        self.total_a_wait_times = []
        self.total_b_wait_times = []
        self.preprocess_times = []
        
    def sensor_a_generator(self):
        """Generate sensor A messages every (9 ± 4) seconds"""
        while True:
            interarrival = random.uniform(SENSOR_A_MEAN - SENSOR_A_VAR, 
                                         SENSOR_A_MEAN + SENSOR_A_VAR)
            yield self.env.timeout(interarrival)
            
            self.sensor_a_generated += 1
            msg_id = f"A-{self.sensor_a_generated}"
            msg_type = 'A'
            arrival_time = self.env.now
            
            print(f"[{self.env.now:.2f}] Sensor {msg_id} arrived")
            
            # Add to task buffer
            self.task_buffer.append((msg_id, msg_type, arrival_time))
            
            # Check if we have 4 messages for a task
            if len(self.task_buffer) >= MESSAGES_PER_TASK:
                task_messages = self.task_buffer[:MESSAGES_PER_TASK]
                self.task_buffer = self.task_buffer[MESSAGES_PER_TASK:]
                self.env.process(self.preprocess_task(task_messages))
    
    def sensor_b_generator(self):
        """Generate sensor B messages every 2 seconds"""
        while True:
            yield self.env.timeout(SENSOR_B_INTERVAL)
            
            self.sensor_b_generated += 1
            msg_id = f"B-{self.sensor_b_generated}"
            msg_type = 'B'
            arrival_time = self.env.now
            
            print(f"[{self.env.now:.2f}] Sensor {msg_id} arrived")
            
            # Add to task buffer
            self.task_buffer.append((msg_id, msg_type, arrival_time))
            
            # Check if we have 4 messages for a task
            if len(self.task_buffer) >= MESSAGES_PER_TASK:
                task_messages = self.task_buffer[:MESSAGES_PER_TASK]
                self.task_buffer = self.task_buffer[MESSAGES_PER_TASK:]
                self.env.process(self.preprocess_task(task_messages))
    
    def preprocess_task(self, task_messages):
        """Preprocess a task of 4 messages using one of 2 processors"""
        task_start = self.env.now
        
        # Randomly choose processor
        if random.random() < 0.5:
            processor = self.processor1
            proc_name = "Processor1"
        else:
            processor = self.processor2
            proc_name = "Processor2"
        
        with processor.request() as req:
            yield req
            
            print(f"[{self.env.now:.2f}] Task starting preprocessing on {proc_name}")
            yield self.env.timeout(PREPROCESS_TIME)
            
            # Check for failure (24% chance)
            if random.random() < PREPROCESS_FAILURE_RATE:
                # Task failed, needs retry
                self.tasks_failed += 1
                print(f"[{self.env.now:.2f}] Task FAILED on {proc_name}, retrying...")
                # Retry immediately (as per problem statement)
                yield self.env.timeout(PREPROCESS_TIME)  # Retry takes same time
                self.tasks_retried += 1
                print(f"[{self.env.now:.2f}] Task RETRY completed on {proc_name}")
            
            self.tasks_preprocessed += 1
            preprocess_time = self.env.now - task_start
            self.preprocess_times.append(preprocess_time)
            
            print(f"[{self.env.now:.2f}] Task preprocessing complete on {proc_name} "
                  f"(time: {preprocess_time:.2f}s)")
            
            # Distribute messages to appropriate buffers
            for msg_id, msg_type, arrival_time in task_messages:
                wait_time = self.env.now - arrival_time
                
                if msg_type == 'A':
                    self.preprocessed_a.append((msg_id, self.env.now))
                    self.total_a_wait_times.append(wait_time)
                    print(f"[{self.env.now:.2f}] Message {msg_id} ready for distribution")
                else:
                    self.preprocessed_b.append((msg_id, self.env.now))
                    self.total_b_wait_times.append(wait_time)
                    print(f"[{self.env.now:.2f}] Message {msg_id} ready for distribution")
                
                # Check if we can form a distribution batch (2A + 3B)
                self.try_distribute()
    
    def try_distribute(self):
        """Try to form a distribution batch with 2A + 3B messages"""
        if len(self.preprocessed_a) >= A_FOR_DISTRIBUTION and \
           len(self.preprocessed_b) >= B_FOR_DISTRIBUTION:
            
            # Take 2 A messages and 3 B messages
            batch_a = self.preprocessed_a[:A_FOR_DISTRIBUTION]
            batch_b = self.preprocessed_b[:B_FOR_DISTRIBUTION]
            
            self.preprocessed_a = self.preprocessed_a[A_FOR_DISTRIBUTION:]
            self.preprocessed_b = self.preprocessed_b[B_FOR_DISTRIBUTION:]
            
            self.env.process(self.distribute_and_store(batch_a, batch_b))
    
    def distribute_and_store(self, batch_a, batch_b):
        """Distribute messages and store in dual storage"""
        print(f"[{self.env.now:.2f}] Distribution batch forming: "
              f"2A + 3B = {len(batch_a) + len(batch_b)} messages")
        
        # Store each message in both main and backup storage
        all_messages = batch_a + batch_b
        
        for msg_id, ready_time in all_messages:
            # Store in main storage
            with self.main_storage.request() as req:
                yield req
                yield self.env.timeout(STORAGE_TIME)
            
            # Store in backup storage (parallel or sequential)
            with self.backup_storage.request() as req:
                yield req
                yield self.env.timeout(STORAGE_TIME)
            
            self.messages_stored += 1
            print(f"[{self.env.now:.2f}] Message {msg_id[0]} stored in dual storage")
        
        self.distributions_completed += 1
        print(f"[{self.env.now:.2f}] Distribution batch complete: "
              f"{len(all_messages)} messages stored")


def run_simulation():
    """Run the data collection system simulation"""
    print("=" * 70)
    print("TOPIC 9: DATA COLLECTION SYSTEM SIMULATION")
    print("=" * 70)
    print(f"Simulation time: {SIMULATION_HOURS} hours ({SIMULATION_TIME} seconds)")
    print(f"Sensor A: every ({SENSOR_A_MEAN} +/- {SENSOR_A_VAR}) seconds")
    print(f"Sensor B: every {SENSOR_B_INTERVAL} seconds")
    print(f"Task size: {MESSAGES_PER_TASK} messages")
    print(f"Preprocessing: {PREPROCESS_TIME}s, {PREPROCESS_FAILURE_RATE*100:.0f}% failure rate")
    print(f"Distribution: {A_FOR_DISTRIBUTION}A + {B_FOR_DISTRIBUTION}B messages")
    print("=" * 70)
    
    env = simpy.Environment()
    system = DataCollectionSystem(env)
    
    # Start sensor generators
    env.process(system.sensor_a_generator())
    env.process(system.sensor_b_generator())
    
    # Run simulation
    env.run(until=SIMULATION_TIME)
    
    # Print results
    print("\n" + "=" * 70)
    print("SIMULATION RESULTS")
    print("=" * 70)
    
    print(f"\nSensor Statistics:")
    print(f"  Sensor A messages generated: {system.sensor_a_generated}")
    print(f"  Sensor B messages generated: {system.sensor_b_generated}")
    print(f"  Total messages: {system.sensor_a_generated + system.sensor_b_generated}")
    
    print(f"\nPreprocessing Statistics:")
    print(f"  Tasks preprocessed: {system.tasks_preprocessed}")
    print(f"  Tasks failed (24%): {system.tasks_failed}")
    print(f"  Tasks retried: {system.tasks_retried}")
    if system.preprocess_times:
        print(f"  Average preprocessing time: {np.mean(system.preprocess_times):.2f}s")
    
    print(f"\nDistribution Statistics:")
    print(f"  Distribution batches completed: {system.distributions_completed}")
    print(f"  Messages stored in dual storage: {system.messages_stored}")
    
    print(f"\nWait Times:")
    if system.total_a_wait_times:
        print(f"  Sensor A: avg={np.mean(system.total_a_wait_times):.2f}s, "
              f"max={np.max(system.total_a_wait_times):.2f}s")
    if system.total_b_wait_times:
        print(f"  Sensor B: avg={np.mean(system.total_b_wait_times):.2f}s, "
              f"max={np.max(system.total_b_wait_times):.2f}s")
    
    print(f"\nBuffer Status at end:")
    print(f"  Messages in task buffer: {len(system.task_buffer)}")
    print(f"  Preprocessed A waiting: {len(system.preprocessed_a)}")
    print(f"  Preprocessed B waiting: {len(system.preprocessed_b)}")
    
    # Save results
    with open('simulation_results.txt', 'w', encoding='utf-8') as f:
        f.write("Topic 9 Data Collection System Results\n")
        f.write("=" * 50 + "\n")
        f.write(f"Simulation time: {SIMULATION_HOURS} hours\n")
        f.write(f"Sensor A generated: {system.sensor_a_generated}\n")
        f.write(f"Sensor B generated: {system.sensor_b_generated}\n")
        f.write(f"Tasks preprocessed: {system.tasks_preprocessed}\n")
        f.write(f"Tasks failed: {system.tasks_failed}\n")
        f.write(f"Distributions completed: {system.distributions_completed}\n")
        f.write(f"Messages stored: {system.messages_stored}\n")
    
    return system


if __name__ == "__main__":
    random.seed(42)
    run_simulation()
