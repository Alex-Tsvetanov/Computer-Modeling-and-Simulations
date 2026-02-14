import simpy
import random
import numpy as np

# Configuration
SIMULATION_TIME = 0
MAX_MESSAGES = 250  # Simulation ends after 250 messages

# Parameters
ARRIVAL_MEAN = 3.0
ARRIVAL_DEV = 1.0   # 3 +/- 1 -> Normal? Text says "3 +/- 1 s". Usually implies Uniform or Normal. 
                    # Context of other problems: "Normal ... base value 2 ... max deviation 1".
                    # Here it just says "3 +/- 1". Let's assume Uniform(2, 4) or Normal(3, 1/3)?
                    # "3 ± 1" usually implies Uniform[2, 4] in this context or Normal with 3sigma=1?
                    # Problem 1 says "Normal ... base 2 ... max dev 1". 
                    # Problem 2 (Topic 31 is Problem 31, same text as Problem 2?)
                    # Problem 31 text: "на всеки (3 ± 1) s."
                    # Problem 2 text: "на всеки (3 ± 1) s."
                    # Let's look at Problem 1: "(8 ± 1) seconds ... Normal ... base 2 ... max dev 1". 
                    # Wait, Problem 1 has two times? "received block ... every (8 +/- 1)s". 
                    # "Recording ... Normal ... base 2 ... max dev 1".
                    # So "(X ± Y)" likely means Uniform in [X-Y, X+Y] in this specific problem set context, 
                    # unless specified otherwise like in Problem 1's second part.
                    # For Topic 31: "received every (3 ± 1) s". Let's use Uniform(2, 4).
                    # "Processing ... is (5 ± 2) s". Let's use Uniform(3, 7).

ARRIVAL_MIN = 2.0
ARRIVAL_MAX = 4.0

PROCESS_MIN = 3.0
PROCESS_MAX = 7.0

DEADLINE = 12.0 # Max wait time in buffer. 
# "Dynamic of process is such that it makes sense to process messages that wait no more than 12s in buffer."
# "The rest are considered lost."

class Monitor:
    def __init__(self):
        self.generated_count = 0
        self.processed_count = 0
        self.lost_count = 0
        self.wait_times = []
        
monitor = Monitor()

def process_message(env, name, server):
    arrival_time = env.now
    
    # Message enters buffer
    # It waits for server. 
    # But we need to check if it waits too long.
    # SimPy Resource request: we can't easily check "time in queue" before getting resource unless we use Reneging.
    
    with server.request() as req:
        # We wait for the server OR until deadline is reached relative to arrival?
        # The constraint is: "wait no more than 12s in buffer".
        # So if (now - arrival) > 12 when we *would* start processing, we drop it?
        # Or do we drop it exactly at arrival+12 if not started?
        # Usually "reneging" means leaving queue at strict time.
        
        results = yield req | env.timeout(DEADLINE)
        
        if req in results:
            # We got the server
            wait_time = env.now - arrival_time
            # Double check constraint just in case (though timeout handles it)
            if wait_time > DEADLINE:
                # This path shouldn't theoretically be reached if timeout works, 
                # but with | operator, if both happen same tick... 
                monitor.lost_count += 1
            else:
                monitor.wait_times.append(wait_time)
                # Process
                proc_time = random.uniform(PROCESS_MIN, PROCESS_MAX)
                yield env.timeout(proc_time)
                monitor.processed_count += 1
        else:
            # Timed out (waited > 12s)
            monitor.lost_count += 1
            # We don't process it.

def source(env, server):
    i = 0
    while monitor.generated_count < MAX_MESSAGES:
        # Inter-arrival
        dt = random.uniform(ARRIVAL_MIN, ARRIVAL_MAX)
        yield env.timeout(dt)
        
        i += 1
        monitor.generated_count += 1
        env.process(process_message(env, f'Msg-{i}', server))

def main():
    env = simpy.Environment()
    server = simpy.Resource(env, capacity=1)
    
    env.process(source(env, server))
    
    # Run until we generate 250 messages and they clear out (or enough time passes)
    # We can just run for a long time, the source stops at 250.
    env.run() 
    
    print(f"Simulation ended at {env.now:.2f}s")
    print("-" * 30)
    print("Topic 31 Results:")
    print(f"  Messages Generated: {monitor.generated_count}")
    print(f"  Messages Processed: {monitor.processed_count}")
    print(f"  Messages Lost (Timeout > 12s): {monitor.lost_count}")
    print(f"  Loss Rate: {monitor.lost_count / monitor.generated_count * 100:.2f}%")
    if monitor.wait_times:
        print(f"  Avg Wait Time (Processed): {np.mean(monitor.wait_times):.4f} s")
        print(f"  Max Wait Time (Processed): {np.max(monitor.wait_times):.4f} s")

    # Save data for TikZ
    with open('topic31_wait_times.txt', 'w') as f:
        for t in monitor.wait_times:
            f.write(f"{t}\n")

if __name__ == "__main__":
    main()
