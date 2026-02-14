"""
Topic 4: Email Processing System Simulation
Complex system with 3 message types and parallel processing workflows.
"""

import simpy
import random
import numpy as np

# Constants
SIMULATION_HOURS = 100
SIMULATION_TIME = SIMULATION_HOURS * 60  # 100 hours in minutes
ARRIVAL_MEAN = 20
ARRIVAL_VAR = 5

class EmailSystem:
    def __init__(self, env):
        self.env = env
        self.total_emails = 0
        self.simple_processed = 0
        self.spam_deleted = 0
        self.complex_processed = 0
        self.completed_orders = 0
        
        # Statistics
        self.simple_times = []
        self.spam_times = []
        self.complex_total_times = []
        
    def email_arrival(self):
        """Generate emails according to uniform distribution"""
        while True:
            interarrival = random.uniform(ARRIVAL_MEAN - ARRIVAL_VAR, ARRIVAL_MEAN + ARRIVAL_VAR)
            yield self.env.timeout(interarrival)
            
            self.total_emails += 1
            email_id = self.total_emails
            
            # Determine email type (1/3 each)
            email_type = random.randint(1, 3)
            
            if email_type == 1:
                # Simple email - processed in 60 min
                self.env.process(self.process_simple_email(email_id))
            elif email_type == 2:
                # Spam - deleted in 1 min
                self.env.process(self.process_spam(email_id))
            else:
                # Complex email - requires preprocessing and parallel assembly
                self.env.process(self.process_complex_email(email_id))
    
    def process_simple_email(self, email_id):
        """Type 1: Simple processing within 60 minutes"""
        start_time = self.env.now
        processing_time = 60
        yield self.env.timeout(processing_time)
        
        self.simple_processed += 1
        self.simple_times.append(self.env.now - start_time)
        print(f"[{self.env.now:.2f}] Simple email {email_id} processed and response sent")
    
    def process_spam(self, email_id):
        """Type 2: Spam deleted within 1 minute"""
        start_time = self.env.now
        yield self.env.timeout(1)
        
        self.spam_deleted += 1
        self.spam_times.append(self.env.now - start_time)
        print(f"[{self.env.now:.2f}] Spam email {email_id} deleted")
    
    def process_complex_email(self, email_id):
        """Type 3: Complex email with preprocessing and parallel assembly"""
        start_time = self.env.now
        
        # Preprocessing: 30 minutes
        yield self.env.timeout(30)
        print(f"[{self.env.now:.2f}] Email {email_id} preprocessing complete, requesting confirmations...")
        
        # Wait for two confirmation messages (from supplier and warehouse)
        # These arrive and are processed in parallel
        confirmation_results = yield simpy.AllOf(self.env, [
            self.env.process(self.get_supplier_confirmation(email_id)),
            self.env.process(self.get_warehouse_confirmation(email_id))
        ])
        
        print(f"[{self.env.now:.2f}] Email {email_id} all confirmations received, assembling response...")
        
        # Parallel assembly: client response (60±2 min) and other two (60±8 min)
        yield simpy.AllOf(self.env, [
            self.env.process(self.assemble_client_response(email_id)),
            self.env.process(self.assemble_other_responses(email_id))
        ])
        
        self.complex_processed += 1
        total_time = self.env.now - start_time
        self.complex_total_times.append(total_time)
        self.completed_orders += 1
        print(f"[{self.env.now:.2f}] Email {email_id} COMPLETED. Total time: {total_time:.2f} min")
    
    def get_supplier_confirmation(self, email_id):
        """Get confirmation from supplier"""
        # This is part of the parallel confirmation collection
        yield self.env.timeout(0)  # Arrives immediately after preprocessing
        print(f"[{self.env.now:.2f}] Email {email_id} supplier confirmation received")
    
    def get_warehouse_confirmation(self, email_id):
        """Get confirmation from warehouse"""
        # This is part of the parallel confirmation collection
        yield self.env.timeout(0)  # Arrives immediately after preprocessing
        print(f"[{self.env.now:.2f}] Email {email_id} warehouse confirmation received")
    
    def assemble_client_response(self, email_id):
        """Assemble client response: 60 ± 2 minutes"""
        processing_time = random.uniform(60 - 2, 60 + 2)
        yield self.env.timeout(processing_time)
        print(f"[{self.env.now:.2f}] Email {email_id} client response assembled")
    
    def assemble_other_responses(self, email_id):
        """Assemble other two responses: 60 ± 8 minutes (parallel)"""
        processing_time = random.uniform(60 - 8, 60 + 8)
        yield self.env.timeout(processing_time)
        print(f"[{self.env.now:.2f}] Email {email_id} other responses assembled")


def run_simulation():
    """Run the email processing simulation"""
    print("=" * 60)
    print("TOPIC 4: EMAIL PROCESSING SYSTEM SIMULATION")
    print("=" * 60)
    
    env = simpy.Environment()
    system = EmailSystem(env)
    
    # Start email arrival process
    env.process(system.email_arrival())
    
    # Run simulation for 100 hours
    env.run(until=SIMULATION_TIME)
    
    # Print results
    print("\n" + "=" * 60)
    print("SIMULATION RESULTS")
    print("=" * 60)
    print(f"Total simulation time: {SIMULATION_HOURS} hours ({SIMULATION_TIME} minutes)")
    print(f"\nEmail Statistics:")
    print(f"  Total emails received: {system.total_emails}")
    print(f"  Simple emails processed (Type 1): {system.simple_processed}")
    print(f"  Spam deleted (Type 2): {system.spam_deleted}")
    print(f"  Complex orders completed (Type 3): {system.completed_orders}")
    print(f"  Complex emails still in progress: {system.complex_processed - system.completed_orders}")
    
    # Expected distribution: ~33% each
    expected_each = system.total_emails / 3
    print(f"\nExpected ~{expected_each:.0f} emails of each type")
    
    if system.simple_times:
        print(f"\nSimple Email Processing:")
        print(f"  Average time: {np.mean(system.simple_times):.2f} min")
    
    if system.spam_times:
        print(f"\nSpam Deletion:")
        print(f"  Average time: {np.mean(system.spam_times):.2f} min")
    
    if system.complex_total_times:
        print(f"\nComplex Email Processing:")
        print(f"  Average total time: {np.mean(system.complex_total_times):.2f} min")
        print(f"  Min time: {np.min(system.complex_total_times):.2f} min")
        print(f"  Max time: {np.max(system.complex_total_times):.2f} min")
        # Expected: 30 + max(60±2, 60±8) ≈ 30 + 68 = 98 min minimum
    
    # Save results for LaTeX report
    with open('simulation_results.txt', 'w', encoding='utf-8') as f:
        f.write("Topic 4 Email Processing Simulation Results\n")
        f.write("=" * 50 + "\n")
        f.write(f"Simulation time: {SIMULATION_HOURS} hours\n")
        f.write(f"Total emails: {system.total_emails}\n")
        f.write(f"Simple processed: {system.simple_processed}\n")
        f.write(f"Spam deleted: {system.spam_deleted}\n")
        f.write(f"Complex completed: {system.completed_orders}\n")
        if system.complex_total_times:
            f.write(f"Avg complex time: {np.mean(system.complex_total_times):.2f} min\n")
    
    print("\nResults saved to simulation_results.txt")
    return system


if __name__ == "__main__":
    random.seed(42)
    run_simulation()
