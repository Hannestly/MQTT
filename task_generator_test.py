import paho.mqtt.client as mqtt
import time
import json
import random
from threading import Thread
import copy
import statistics

class TaskGeneratorTest:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="task_generator_test")
        self.results = []
        
        # Task set tracking
        self.current_task_set = None
        self.task_set_metrics = {}
        self.pending_tasks = set()
        self.task_deadlines = {}  # Store deadlines for each task
        self.test_complete = False
        self.test_round = 0
        self.max_rounds = 5
        self.test_results = []
        self.rest_period = 5  # 5 seconds rest between trials
        
        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Task Generator Test connected with result code {rc}")
        # Subscribe to task status updates
        client.subscribe("task/status")
        client.subscribe("task/+/ack")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic.startswith("task/") and topic.endswith("/ack"):
            # Task acknowledgment
            print(f"Task {data['task_id']} acknowledged by broker")
        elif topic == "task/status":
            # Task status update
            self.results.append(data)
            task_id = data['task_id']
            status = data['status']
            
            # Track task completion for the current set
            if task_id in self.pending_tasks:
                self.pending_tasks.remove(task_id)
                
                # Calculate completion time
                if self.current_task_set:
                    set_id = self.current_task_set
                    if set_id in self.task_set_metrics:
                        metrics = self.task_set_metrics[set_id]
                        
                        # Record individual task completion time
                        start_time = metrics["start_time"]
                        completion_time = time.time() - start_time
                        metrics["task_completion_times"][task_id] = completion_time
                        
                        # Check if deadline was met
                        deadline_met = False
                        if status == "completed":
                            # Get the deadline for this task
                            if task_id in self.task_deadlines:
                                task_deadline = self.task_deadlines[task_id]
                                metrics["task_deadlines"][task_id] = task_deadline
                                
                                # Task is only counted as completed if it met its deadline
                                if completion_time <= task_deadline:
                                    deadline_met = True
                                    metrics["completed_tasks"] += 1
                                    print(f"Task {task_id} completed WITHIN deadline ({completion_time:.2f}s <= {task_deadline:.2f}s)")
                                else:
                                    metrics["missed_deadlines"] += 1
                                    print(f"Task {task_id} MISSED deadline ({completion_time:.2f}s > {task_deadline:.2f}s)")
                        
                        if status != "completed":
                            metrics["failed_tasks"] += 1
                            print(f"Task {task_id} failed with status: {status}")
                        
                        # Check if all tasks in the set are processed
                        if len(self.pending_tasks) == 0:
                            # Calculate the actual trial time (from start to last task completion)
                            trial_end_time = time.time()
                            metrics = self.task_set_metrics[self.current_task_set]
                            trial_start_time = metrics["start_time"]
                            
                            # Calculate actual trial duration
                            trial_time = trial_end_time - trial_start_time
                            metrics["trial_time"] = trial_time
                            
                            # Log completion with timestamps for verification
                            print(f"\n[TRIAL COMPLETED] Round {self.test_round}")
                            print(f"  Start time: {time.strftime('%H:%M:%S', time.localtime(trial_start_time))}")
                            print(f"  End time: {time.strftime('%H:%M:%S', time.localtime(trial_end_time))}")
                            print(f"  Actual duration: {trial_time:.3f}s")
                            
                            # Calculate average completion time for successful tasks
                            successful_times = [time for task_id, time in metrics["task_completion_times"].items() 
                                               if task_id in self.task_deadlines and 
                                               time <= self.task_deadlines[task_id]]
                            
                            if successful_times:
                                avg_time = sum(successful_times) / len(successful_times)
                                metrics["avg_completion_time"] = avg_time
                            else:
                                metrics["avg_completion_time"] = None
                            
                            print(f"\n[TEST ROUND {self.test_round}/{self.max_rounds}] {metrics['completed_tasks']}/{metrics['total_tasks']} tasks met deadlines")
                            print(f"Missed deadlines: {metrics['missed_deadlines']}, Failed tasks: {metrics['failed_tasks']}")
                            print(f"Trial completion time: {trial_time:.2f}s (time to process entire task set)")
                            
                            if metrics["avg_completion_time"] is not None:
                                print(f"Average time for successful tasks: {metrics['avg_completion_time']:.2f}s")
                            else:
                                print(f"No tasks met deadlines")
                            
                            # Save the results of this round
                            self.test_results.append({
                                "round": self.test_round,
                                "completed_tasks": metrics["completed_tasks"],
                                "total_tasks": metrics["total_tasks"],
                                "missed_deadlines": metrics["missed_deadlines"],
                                "failed_tasks": metrics["failed_tasks"],
                                "trial_time": trial_time,  # Renamed for clarity
                                "avg_task_time": metrics["avg_completion_time"]
                            })
                            
                            # Mark this test as complete
                            self.test_complete = True

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by Task Generator Test")

    def start(self):
        """Start the task generator test"""
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print("Task Generator Test started")
        self.mqtt_client.loop_start()

    def submit_tasks(self, task_set):
        """Submit all tasks simultaneously to test scheduling algorithms"""
        scheduling = task_set.get("scheduling", "RM")
        tasks = task_set.get("tasks", [])

        # Add timestamp for all tasks
        current_time = time.time()
        task_ids = set()  # Track task IDs to avoid duplicates
        
        # Filter out any duplicate task IDs
        unique_tasks = []
        for task in tasks:
            task_id = task.get("task_id")
            if task_id not in task_ids:
                task_ids.add(task_id)
                task["submission_time"] = current_time
                unique_tasks.append(task)
                
                # Store the deadline for this task
                deadline = task.get("deadline", float('inf'))
                self.task_deadlines[task_id] = deadline
            else:
                print(f"Warning: Skipping duplicate task ID: {task_id}")
        
        # Send all tasks in one batch
        task_data = {
            "scheduling": scheduling,
            "tasks": unique_tasks,
            "batch_submission": True,
            "timestamp": current_time
        }
        
        # Setup tracking for this task set
        set_id = f"{scheduling}_{task_set.get('type', 'unknown')}_{current_time}"
        self.current_task_set = set_id
        self.pending_tasks = set(task.get("task_id") for task in unique_tasks)
        
        # Record the task set start time
        trial_start_time = time.time()
        
        # Initialize metrics for this set
        self.task_set_metrics[set_id] = {
            "scheduling": scheduling,
            "type": task_set.get("type", "unknown"),
            "start_time": trial_start_time,
            "total_tasks": len(unique_tasks),
            "completed_tasks": 0,  # Tasks that met their deadline
            "missed_deadlines": 0, # Tasks that completed but missed deadline
            "failed_tasks": 0,     # Tasks that failed to complete
            "task_completion_times": {},
            "task_deadlines": {},
            "trial_time": None,  # Will be set when all tasks complete
            "avg_completion_time": None
        }
        
        # Publish the entire batch
        self.mqtt_client.publish("task/submit", json.dumps(task_data))
        
        # Log submission information
        print(f"\n[TEST ROUND {self.test_round}/{self.max_rounds}] Sent {len(unique_tasks)} tasks with {scheduling} scheduling")
        print(f"  Trial start time: {time.strftime('%H:%M:%S', time.localtime(trial_start_time))}")
        print(f"  Light tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'light')}")
        print(f"  Medium tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'medium')}")
        print(f"  Heavy tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'heavy')}")

    def run_test_batch(self, task_set, max_rounds=5):
        """Run the same task set multiple times and collect statistics"""
        self.max_rounds = max_rounds
        original_scheduling = task_set.get("scheduling", "RM")
        task_set_type = task_set.get("type", "unknown")
        
        print(f"\n=== STARTING TEST BATCH: {original_scheduling} on {task_set_type} ===")
        print(f"Running {max_rounds} test rounds with {self.rest_period}s rest between rounds...")
        
        self.test_results = []
        
        for round_num in range(1, max_rounds + 1):
            # Reset for this round
            self.test_round = round_num
            self.test_complete = False
            self.pending_tasks = set()
            self.task_deadlines = {}
            
            # Create a deep copy of the task set to avoid modifying the original
            current_task_set = copy.deepcopy(task_set)
            
            # Regenerate task IDs to ensure uniqueness across rounds
            for i, task in enumerate(current_task_set.get("tasks", [])):
                # Keep the original task ID format but add round number
                original_id = task.get("task_id", f"T{i}")
                if original_id.startswith("T"):
                    task_number = original_id[1:]
                    task["task_id"] = f"T{task_number}_R{round_num}"
                else:
                    task["task_id"] = f"{original_id}_R{round_num}"
            
            # Submit the tasks for this round
            self.submit_tasks(current_task_set)
            
            # Wait for this round to complete
            print(f"Waiting for round {round_num} to complete...")
            timeout = 60  # 60 second timeout per round
            start_time = time.time()
            
            while not self.test_complete and time.time() - start_time < timeout:
                time.sleep(1)
            
            if not self.test_complete:
                # Calculate the actual elapsed time from start to timeout
                if self.current_task_set in self.task_set_metrics:
                    metrics = self.task_set_metrics[self.current_task_set]
                    trial_start_time = metrics["start_time"]
                    trial_time = time.time() - trial_start_time
                    
                    print(f"\n[TRIAL TIMEOUT] Round {round_num}")
                    print(f"  Start time: {time.strftime('%H:%M:%S', time.localtime(trial_start_time))}")
                    print(f"  Timeout at: {time.strftime('%H:%M:%S', time.localtime(time.time()))}")
                    print(f"  Actual duration: {trial_time:.3f}s")
                    print(f"  Tasks completed: {metrics['completed_tasks']}/{metrics['total_tasks']}")
                    print(f"  Tasks remaining: {len(self.pending_tasks)}")
                    
                    # Set the actual trial time, not just the timeout value
                    metrics["trial_time"] = trial_time
                    metrics["timed_out"] = True
                    
                    # Save remaining task IDs for debugging
                    metrics["pending_tasks"] = list(self.pending_tasks)
                    
                    # Append detailed results
                    self.test_results.append({
                        "round": round_num,
                        "completed_tasks": metrics["completed_tasks"],
                        "total_tasks": metrics["total_tasks"],
                        "missed_deadlines": metrics["missed_deadlines"],
                        "failed_tasks": metrics["failed_tasks"],
                        "pending_tasks": len(self.pending_tasks),
                        "trial_time": trial_time,
                        "avg_task_time": metrics["avg_completion_time"],
                        "timed_out": True
                    })
            
            # Print pending task information
            self.print_pending_tasks()
            
            # Add rest period between trials if not the last round
            if round_num < max_rounds:
                rest_start = time.time()
                print(f"\n[SYSTEM REST] Allowing system to rest for {self.rest_period} seconds...")
                
                # Display a countdown timer
                for i in range(self.rest_period, 0, -1):
                    print(f"  Resuming in {i} seconds...", end="\r")
                    time.sleep(1)
                
                actual_rest = time.time() - rest_start
                print(f"\n[SYSTEM REST] Completed rest period of {actual_rest:.1f} seconds")
        
        # Report aggregate results
        self.report_test_batch_results(original_scheduling, task_set_type)
        
        print("\n=== TEST BATCH COMPLETE - RETURNING TO MENU ===")
        return  # This return ensures the function completes cleanly

    def report_test_batch_results(self, scheduling, task_set_type):
        """Report aggregate results from all test rounds"""
        if not self.test_results:
            print("\nNo test results to report")
            return
        
        print(f"\n=== AGGREGATE RESULTS: {scheduling} on {task_set_type} ===")
        print(f"Completed {len(self.test_results)} test rounds with {self.rest_period}s rest between rounds")
        
        # Calculate statistics across all rounds
        completed_tasks = [r["completed_tasks"] for r in self.test_results]
        total_tasks = [r["total_tasks"] for r in self.test_results]
        trial_times = [r["trial_time"] for r in self.test_results]  # Time to complete each trial
        
        # Calculate success rates
        success_rates = [c / t * 100 if t > 0 else 0 for c, t in zip(completed_tasks, total_tasks)]
        
        # Print detailed results for each round
        print("\nDetailed round results:")
        print("Round | Tasks Met/Total | Success Rate | Trial Time | Status")
        print("-" * 65)
        
        for i, result in enumerate(self.test_results):
            status = "TIMEOUT" if result.get("timed_out", False) else "Complete"
            print(f"{i+1:5d} | {result['completed_tasks']:4d}/{result['total_tasks']:<6d} | {success_rates[i]:6.1f}% | {result['trial_time']:.3f}s | {status}")
        
        # Add timeout stats
        timeout_count = sum(1 for r in self.test_results if r.get("timed_out", False))
        if timeout_count > 0:
            print(f"\nTimeouts: {timeout_count}/{len(self.test_results)} rounds timed out")
            
            # Calculate average completion percentage for timed out rounds
            timeout_completion = [r["completed_tasks"]/r["total_tasks"]*100 for r in self.test_results if r.get("timed_out", False)]
            print(f"Average completion before timeout: {statistics.mean(timeout_completion):.1f}%")
        
        # Print aggregate statistics
        print("\nAggregate statistics:")
        print(f"Avg tasks meeting deadline: {statistics.mean(completed_tasks):.1f} out of {statistics.mean(total_tasks):.1f}")
        print(f"Avg success rate: {statistics.mean(success_rates):.1f}%")
        print(f"Avg trial completion time: {statistics.mean(trial_times):.2f}s (time to process entire task set)")
        
        if len(trial_times) > 1:
            print(f"Min trial time: {min(trial_times):.2f}s")
            print(f"Max trial time: {max(trial_times):.2f}s")
            print(f"Std dev of trial time: {statistics.stdev(trial_times):.2f}s")
        
        print(f"\n=== END OF TEST BATCH: {scheduling} on {task_set_type} ===")

    def get_results(self):
        """Get the task execution results"""
        return self.results

    def get_task_set_metrics(self):
        """Get the metrics for completed task sets"""
        return self.task_set_metrics

    def load_task_sets(self, filename="task_sets.json"):
        """Load task sets from JSON file"""
        try:
            with open(filename, 'r') as f:
                task_sets = json.load(f)
            return task_sets
        except FileNotFoundError:
            print(f"Error: {filename} not found")
            return None
        except json.JSONDecodeError:
            print(f"Error: {filename} contains invalid JSON")
            return None

    def print_pending_tasks(self):
        """Print information about tasks that haven't completed yet"""
        if not self.pending_tasks:
            return
        
        print(f"\n[PENDING TASKS] {len(self.pending_tasks)} tasks not completed:")
        
        # Group by task type if possible
        task_types = {}
        
        for task_id in self.pending_tasks:
            # Extract information about the task type if possible
            task_type = "unknown"
            if task_id in self.task_deadlines:
                for task in self.results:
                    if task["task_id"] == task_id:
                        task_type = task.get("load_type", "unknown")
                        break
                    
            if task_type not in task_types:
                task_types[task_type] = []
            task_types[task_type].append(task_id)
        
        # Print summary by type
        for task_type, ids in task_types.items():
            print(f"  {task_type}: {len(ids)} tasks")
            if len(ids) <= 10:  # Only print IDs if not too many
                print(f"    IDs: {', '.join(ids)}")


if __name__ == "__main__":
    generator = TaskGeneratorTest()
    generator.start()

    try:
        is_testing = False  # Flag to track if a test is in progress
        
        while True:
            # Only show menu when not currently testing
            if not is_testing:
                print("\nScheduling Options:")
                print("1. Rate Monotonic (RM)")
                print("2. Earliest Deadline First (EDF)")
                print("3. Round Robin (RR)")
                print("4. Compare All Algorithms")
                print("5. Exit")

                choice = input("Enter scheduling choice: ")

                if choice == "5":
                    break
                    
                if choice == "4":
                    # Run all scheduling algorithms on both task sets
                    is_testing = True
                    scheduling_algorithms = ["RM", "EDF", "RR"]
                    task_sets = generator.load_task_sets()
                    if not task_sets:
                        print("No task sets found")
                        is_testing = False
                        continue
                    
                    for set_idx, task_set_type in enumerate(["Mixed Utilization", "Deadline Intensity"]):
                        print(f"\n=== Testing all algorithms on {task_set_type} ===")
                        for algorithm in scheduling_algorithms:
                            # Get the base task set
                            base_task_set = task_sets["task_sets"][set_idx].copy()
                            # Set the scheduling algorithm
                            base_task_set["scheduling"] = algorithm
                            # Run the test batch
                            generator.run_test_batch(base_task_set, max_rounds=5)
                    
                    # Set flag back to false after all tests complete
                    is_testing = False
                    continue
                    
                elif choice not in ["1", "2", "3"]:
                    print("Invalid choice")
                    continue

                # Map scheduling choice to algorithm name
                scheduling_map = {
                    "1": "RM",
                    "2": "EDF",
                    "3": "RR"
                }
                selected_scheduling = scheduling_map[choice]

                print("\nTask Set Types:")
                print("1. Mixed Utilization")
                print("2. Deadline Intensity")

                set_type = input("Enter task set type: ")
                if set_type not in ["1", "2"]:
                    print("Invalid task set type")
                    continue

                # Load task sets
                task_sets = generator.load_task_sets()
                if not task_sets:
                    print("No task sets found")
                    continue

                # Get the selected task set
                try:
                    set_index = int(set_type) - 1
                    selected_task_set = task_sets["task_sets"][set_index].copy()
                    
                    # Add scheduling algorithm to task set
                    selected_task_set["scheduling"] = selected_scheduling
                    
                    # Set testing flag to true
                    is_testing = True
                    
                    # Run the test batch
                    generator.run_test_batch(selected_task_set, max_rounds=5)
                    
                    # Set flag back to false
                    is_testing = False
                    
                except (IndexError, ValueError):
                    print("Invalid task set selection")
                    is_testing = False
                    continue
            else:
                # If we're still testing, just sleep and check again
                time.sleep(1)

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        generator.mqtt_client.loop_stop() 