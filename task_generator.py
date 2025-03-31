import paho.mqtt.client as mqtt
import time
import json
import random
from threading import Thread

class TaskGenerator:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="task_generator")
        self.results = []
        
        # Task set tracking
        self.current_task_set = None
        self.task_set_metrics = {}
        self.pending_tasks = set()
        self.task_deadlines = {}  # Store deadlines for each task
        
        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Task Generator connected with result code {rc}")
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
            print(f"Task {data['task_id']} acknowledged by broker")
        elif topic == "task/status":
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
                            total_time = time.time() - metrics["start_time"]
                            metrics["total_completion_time"] = total_time
                            
                            # Calculate average completion time for successful tasks
                            successful_times = [time for task_id, time in metrics["task_completion_times"].items() 
                                               if task_id in self.task_deadlines and 
                                               time <= self.task_deadlines[task_id]]
                            
                            if successful_times:
                                avg_time = sum(successful_times) / len(successful_times)
                                metrics["avg_completion_time"] = avg_time
                            else:
                                metrics["avg_completion_time"] = None
                            
                            print(f"\n[TASK SET COMPLETED] {metrics['completed_tasks']}/{metrics['total_tasks']} tasks met deadlines")
                            print(f"Missed deadlines: {metrics['missed_deadlines']}, Failed tasks: {metrics['failed_tasks']}")
                            if metrics["avg_completion_time"] is not None:
                                print(f"Total time: {total_time:.2f}s, Average time for successful tasks: {metrics['avg_completion_time']:.2f}s")
                            else:
                                print(f"Total time: {total_time:.2f}s, No tasks met deadlines")

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by Task Generator")

    def start(self):
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print("Task Generator started")
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
        
        # Initialize metrics for this set
        self.task_set_metrics[set_id] = {
            "scheduling": scheduling,
            "type": task_set.get("type", "unknown"),
            "start_time": current_time,
            "total_tasks": len(unique_tasks),
            "completed_tasks": 0,  # Tasks that met their deadline
            "missed_deadlines": 0, # Tasks that completed but missed deadline
            "failed_tasks": 0,     # Tasks that failed to complete
            "task_completion_times": {},
            "task_deadlines": {},
            "total_completion_time": None,
            "avg_completion_time": None
        }
        
        # Publish the entire batch
        self.mqtt_client.publish("task/submit", json.dumps(task_data))
        
        # Log submission information
        print(f"\n[BATCH SUBMISSION] Sent {len(unique_tasks)} tasks with {scheduling} scheduling")
        print(f"  Light tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'light')}")
        print(f"  Medium tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'medium')}")
        print(f"  Heavy tasks: {sum(1 for t in unique_tasks if t.get('load_type') == 'heavy')}")

    def get_results(self):
        return self.results

    def get_task_set_metrics(self):
        """Get the metrics for completed task sets"""
        return self.task_set_metrics

    def analyze_results(self):
        """Analyze the task execution results with focus on deadline adherence"""
        if not self.results:
            print("No results to analyze")
            return

        # Task set metrics with deadline information
        print("\n=== Task Set Performance ===")
        for set_id, metrics in self.task_set_metrics.items():
            scheduling = metrics["scheduling"]
            set_type = metrics["type"]
            total_tasks = metrics["total_tasks"]
            completed_tasks = metrics["completed_tasks"]
            missed_deadlines = metrics["missed_deadlines"]
            failed_tasks = metrics["failed_tasks"]
            
            deadline_met_ratio = completed_tasks / total_tasks * 100 if total_tasks > 0 else 0
            
            print(f"\nTask Set: {set_type} with {scheduling}")
            print(f"  Total tasks: {total_tasks}")
            print(f"  Met deadlines: {completed_tasks} ({deadline_met_ratio:.1f}%)")
            
            if total_tasks > 0:
                missed_pct = missed_deadlines / total_tasks * 100
                failed_pct = failed_tasks / total_tasks * 100
                print(f"  Missed deadlines: {missed_deadlines} ({missed_pct:.1f}%)")
                print(f"  Failed tasks: {failed_tasks} ({failed_pct:.1f}%)")
            
            if metrics["total_completion_time"] is not None:
                print(f"  Total time: {metrics['total_completion_time']:.2f}s")
                
                if metrics["avg_completion_time"] is not None:
                    print(f"  Average time for tasks meeting deadlines: {metrics['avg_completion_time']:.2f}s")
                else:
                    print("  No tasks met their deadlines")
                
                # Calculate min and max task completion times for successful tasks
                successful_times = {task_id: time for task_id, time in metrics["task_completion_times"].items() 
                                  if task_id in metrics["task_deadlines"] and 
                                  time <= metrics["task_deadlines"][task_id]}
                
                if successful_times:
                    min_time = min(successful_times.values())
                    max_time = max(successful_times.values())
                    print(f"  Fastest successful task: {min_time:.2f}s")
                    print(f"  Slowest successful task: {max_time:.2f}s")
                
                # Group by load type with deadline information
                load_types = {}
                for task_id, completion_time in metrics["task_completion_times"].items():
                    # Find the task in results to get its load type
                    for result in self.results:
                        if result["task_id"] == task_id:
                            load_type = result.get("load_type", "unknown")
                            deadline_met = task_id in metrics["task_deadlines"] and completion_time <= metrics["task_deadlines"][task_id]
                            
                            if load_type not in load_types:
                                load_types[load_type] = {"met": [], "missed": []}
                            
                            if deadline_met:
                                load_types[load_type]["met"].append(completion_time)
                            else:
                                load_types[load_type]["missed"].append(completion_time)
                            break
                
                # Print statistics by load type
                if load_types:
                    print("  Performance by load type:")
                    for load_type, times in load_types.items():
                        met_count = len(times["met"])
                        missed_count = len(times["missed"])
                        total = met_count + missed_count
                        
                        if total > 0:
                            met_ratio = met_count / total * 100
                            print(f"    {load_type}: {met_count}/{total} met deadlines ({met_ratio:.1f}%)")
                            
                            if met_count > 0:
                                avg_met = sum(times["met"]) / met_count
                                print(f"      Average time when meeting deadlines: {avg_met:.2f}s")
                            
                            if missed_count > 0:
                                avg_missed = sum(times["missed"]) / missed_count
                                print(f"      Average time when missing deadlines: {avg_missed:.2f}s")

            # Add additional analysis based on task set type
            if set_type == "deadline_intensity":
                self._analyze_deadline_intensity_results(metrics)

    def _analyze_deadline_intensity_results(self, metrics):
        """Analyze results specifically for deadline intensity task sets"""
        print("\n  Deadline intensity performance:")
        
        intensity_stats = {"tight": {"met": 0, "total": 0}, 
                          "moderate": {"met": 0, "total": 0}, 
                          "relaxed": {"met": 0, "total": 0}}
        
        # Collect statistics by deadline intensity
        for task_id, completion_time in metrics["task_completion_times"].items():
            for result in self.results:
                if result["task_id"] == task_id and "deadline_intensity" in result:
                    intensity = result.get("deadline_intensity", "unknown")
                    deadline_met = task_id in metrics["task_deadlines"] and completion_time <= metrics["task_deadlines"][task_id]
                    
                    if intensity in intensity_stats:
                        intensity_stats[intensity]["total"] += 1
                        if deadline_met:
                            intensity_stats[intensity]["met"] += 1
                    break
        
        # Report results by deadline intensity
        for intensity, stats in intensity_stats.items():
            if stats["total"] > 0:
                success_rate = (stats["met"] / stats["total"]) * 100
                print(f"    {intensity.capitalize()} deadlines: {stats['met']}/{stats['total']} met ({success_rate:.1f}%)")

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


if __name__ == "__main__":
    generator = TaskGenerator()
    generator.start()

    try:
        while True:
            print("\nScheduling Options:")
            print("1. Rate Monotonic (RM)")
            print("2. Earliest Deadline First (EDF)")
            print("3. Round Robin (RR)")
            print("4. Analyze results")
            print("5. Exit")

            choice = input("Enter scheduling choice: ")

            if choice == "4":
                generator.analyze_results()
                continue
            elif choice == "5":
                break
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
                
                # Submit tasks
                generator.submit_tasks(selected_task_set)
                
            except (IndexError, ValueError):
                print("Invalid task set selection")
                continue

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        generator.mqtt_client.loop_stop()
