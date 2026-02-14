"""
Topic 20: Three-Computer System with Probabilistic Routing
Complex network with feedback loops:
- Tasks arrive every (3 +/- 1) minutes
- Initial routing: 0.4 to Comp1, 0.3 to Comp2, 0.3 to Comp3
- After Comp1: 0.3 to Comp2, 0.7 to Comp3 (feedback routing)
- After Comp2 or Comp3: task complete
"""

import simpy
import random
import numpy as np

# Constants
NUM_TASKS = 200
ARRIVAL_MEAN = 3
ARRIVAL_VAR = 1

# Processing times (minutes)
T1_MIN = 3  # 4 +/- 1
T1_MAX = 5

T2_MIN = 2  # 3 +/- 1
T2_MAX = 4

T3_MIN = 3  # 5 +/- 2
T3_MAX = 7

# Routing probabilities
P_TO_COMP1 = 0.4
P_TO_COMP2 = 0.3
P_TO_COMP3 = 0.3

P_COMP1_TO_COMP2 = 0.3
P_COMP1_TO_COMP3 = 0.7


class ComputerSystem:
    def __init__(self, env):
        self.env = env
        
        # Three computers as resources
        self.comp1 = simpy.Resource(env, capacity=1)
        self.comp2 = simpy.Resource(env, capacity=1)
        self.comp3 = simpy.Resource(env, capacity=1)
        
        # Statistics
        self.tasks_generated = 0
        self.tasks_completed = 0
        
        # Routing counters
        self.initial_to_comp1 = 0
        self.initial_to_comp2 = 0
        self.initial_to_comp3 = 0
        
        self.comp1_to_comp2 = 0
        self.comp1_to_comp3 = 0
        
        self.completed_at_comp2 = 0
        self.completed_at_comp3 = 0
        
        # Timing statistics
        self.total_times = []
        self.comp1_times = []
        self.comp2_times = []
        self.comp3_times = []
        self.queue_times_comp1 = []
        self.queue_times_comp2 = []
        self.queue_times_comp3 = []
        
    def task_arrival(self):
        """Generate tasks every (3 +/- 1) minutes"""
        while self.tasks_generated < NUM_TASKS:
            interarrival = random.uniform(ARRIVAL_MEAN - ARRIVAL_VAR, 
                                         ARRIVAL_MEAN + ARRIVAL_VAR)
            yield self.env.timeout(interarrival)
            
            self.tasks_generated += 1
            task_id = self.tasks_generated
            arrival_time = self.env.now
            
            print(f"[{self.env.now:.2f}] Task {task_id} arrived")
            
            # Route based on probabilities
            p = random.random()
            if p < P_TO_COMP1:
                self.initial_to_comp1 += 1
                self.env.process(self.process_at_comp1(task_id, arrival_time))
            elif p < P_TO_COMP1 + P_TO_COMP2:
                self.initial_to_comp2 += 1
                self.env.process(self.process_at_comp2(task_id, arrival_time, final=True))
            else:
                self.initial_to_comp3 += 1
                self.env.process(self.process_at_comp3(task_id, arrival_time, final=True))
    
    def process_at_comp1(self, task_id, arrival_time):
        """Process at Computer 1, then route to Comp2 or Comp3"""
        queue_start = self.env.now
        
        with self.comp1.request() as req:
            yield req
            queue_time = self.env.now - queue_start
            self.queue_times_comp1.append(queue_time)
            
            # Processing: 4 +/- 1 minutes
            processing_time = random.uniform(T1_MIN, T1_MAX)
            yield self.env.timeout(processing_time)
            
            self.comp1_times.append(processing_time)
            print(f"[{self.env.now:.2f}] Task {task_id} completed at Computer 1 "
                  f"(queue: {queue_time:.2f} min, process: {processing_time:.2f} min)")
        
        # Route after Comp1: 0.3 to Comp2, 0.7 to Comp3
        p = random.random()
        if p < P_COMP1_TO_COMP2:
            self.comp1_to_comp2 += 1
            self.env.process(self.process_at_comp2(task_id, arrival_time, final=True))
        else:
            self.comp1_to_comp3 += 1
            self.env.process(self.process_at_comp3(task_id, arrival_time, final=True))
    
    def process_at_comp2(self, task_id, arrival_time, final=False):
        """Process at Computer 2, task completes here"""
        queue_start = self.env.now
        
        with self.comp2.request() as req:
            yield req
            queue_time = self.env.now - queue_start
            self.queue_times_comp2.append(queue_time)
            
            # Processing: 3 +/- 1 minutes
            processing_time = random.uniform(T2_MIN, T2_MAX)
            yield self.env.timeout(processing_time)
            
            self.comp2_times.append(processing_time)
            total_time = self.env.now - arrival_time
            
            if final:
                self.completed_at_comp2 += 1
                self.tasks_completed += 1
                self.total_times.append(total_time)
                print(f"[{self.env.now:.2f}] Task {task_id} COMPLETED at Computer 2 "
                      f"(total: {total_time:.2f} min)")
    
    def process_at_comp3(self, task_id, arrival_time, final=False):
        """Process at Computer 3, task completes here"""
        queue_start = self.env.now
        
        with self.comp3.request() as req:
            yield req
            queue_time = self.env.now - queue_start
            self.queue_times_comp3.append(queue_time)
            
            # Processing: 5 +/- 2 minutes
            processing_time = random.uniform(T3_MIN, T3_MAX)
            yield self.env.timeout(processing_time)
            
            self.comp3_times.append(processing_time)
            total_time = self.env.now - arrival_time
            
            if final:
                self.completed_at_comp3 += 1
                self.tasks_completed += 1
                self.total_times.append(total_time)
                print(f"[{self.env.now:.2f}] Task {task_id} COMPLETED at Computer 3 "
                      f"(total: {total_time:.2f} min)")


def run_simulation():
    """Run the three-computer system simulation"""
    print("=" * 70)
    print("TOPIC 20: THREE-COMPUTER SYSTEM WITH PROBABILISTIC ROUTING")
    print("=" * 70)
    print(f"Number of tasks: {NUM_TASKS}")
    print(f"Arrival interval: ({ARRIVAL_MEAN} +/- {ARRIVAL_VAR}) minutes")
    print(f"Initial routing: 0.4->Comp1, 0.3->Comp2, 0.3->Comp3")
    print(f"After Comp1: 0.3->Comp2, 0.7->Comp3")
    print(f"Processing: T1=[{T1_MIN},{T1_MAX}], T2=[{T2_MIN},{T2_MAX}], T3=[{T3_MIN},{T3_MAX}]")
    print("=" * 70)
    
    env = simpy.Environment()
    system = ComputerSystem(env)
    
    # Start task arrival
    env.process(system.task_arrival())
    
    # Run until all tasks are completed
    while system.tasks_completed < NUM_TASKS:
        env.step()
    
    # Print results
    print("\n" + "=" * 70)
    print("SIMULATION RESULTS")
    print("=" * 70)
    
    print(f"\nTask Generation:")
    print(f"  Tasks generated: {system.tasks_generated}")
    print(f"  Tasks completed: {system.tasks_completed}")
    
    print(f"\nInitial Routing:")
    print(f"  To Computer 1: {system.initial_to_comp1} ({system.initial_to_comp1/system.tasks_generated*100:.1f}%)")
    print(f"  To Computer 2: {system.initial_to_comp2} ({system.initial_to_comp2/system.tasks_generated*100:.1f}%)")
    print(f"  To Computer 3: {system.initial_to_comp3} ({system.initial_to_comp3/system.tasks_generated*100:.1f}%)")
    
    print(f"\nRouting from Computer 1:")
    total_from_comp1 = system.comp1_to_comp2 + system.comp1_to_comp3
    print(f"  To Computer 2: {system.comp1_to_comp2} ({system.comp1_to_comp2/total_from_comp1*100:.1f}%)")
    print(f"  To Computer 3: {system.comp1_to_comp3} ({system.comp1_to_comp3/total_from_comp1*100:.1f}%)")
    
    print(f"\nTask Completion:")
    print(f"  At Computer 2: {system.completed_at_comp2} ({system.completed_at_comp2/system.tasks_completed*100:.1f}%)")
    print(f"  At Computer 3: {system.completed_at_comp3} ({system.completed_at_comp3/system.tasks_completed*100:.1f}%)")
    
    print(f"\nTiming Statistics:")
    if system.total_times:
        print(f"  Total time per task: avg={np.mean(system.total_times):.2f} min, "
              f"min={np.min(system.total_times):.2f} min, max={np.max(system.total_times):.2f} min")
    
    if system.queue_times_comp1:
        print(f"  Computer 1 queue: avg={np.mean(system.queue_times_comp1):.2f} min, "
              f"max={np.max(system.queue_times_comp1):.2f} min")
    if system.queue_times_comp2:
        print(f"  Computer 2 queue: avg={np.mean(system.queue_times_comp2):.2f} min, "
              f"max={np.max(system.queue_times_comp2):.2f} min")
    if system.queue_times_comp3:
        print(f"  Computer 3 queue: avg={np.mean(system.queue_times_comp3):.2f} min, "
              f"max={np.max(system.queue_times_comp3):.2f} min")
    
    if system.comp1_times:
        print(f"  Computer 1 processing: avg={np.mean(system.comp1_times):.2f} min")
    if system.comp2_times:
        print(f"  Computer 2 processing: avg={np.mean(system.comp2_times):.2f} min")
    if system.comp3_times:
        print(f"  Computer 3 processing: avg={np.mean(system.comp3_times):.2f} min")
    
    # Save results
    with open('simulation_results.txt', 'w', encoding='utf-8') as f:
        f.write("Topic 20 Three-Computer System Results\n")
        f.write("=" * 50 + "\n")
        f.write(f"Tasks generated: {system.tasks_generated}\n")
        f.write(f"Tasks completed: {system.tasks_completed}\n")
        f.write(f"Initial to Comp1: {system.initial_to_comp1}\n")
        f.write(f"Initial to Comp2: {system.initial_to_comp2}\n")
        f.write(f"Initial to Comp3: {system.initial_to_comp3}\n")
        f.write(f"Comp1->Comp2: {system.comp1_to_comp2}\n")
        f.write(f"Comp1->Comp3: {system.comp1_to_comp3}\n")
        f.write(f"Completed at Comp2: {system.completed_at_comp2}\n")
        f.write(f"Completed at Comp3: {system.completed_at_comp3}\n")
        if system.total_times:
            f.write(f"Avg total time: {np.mean(system.total_times):.2f} min\n")
    
    return system


if __name__ == "__main__":
    random.seed(42)
    run_simulation()
