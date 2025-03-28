# task_generator.py
import paho.mqtt.client as mqtt
import time
import json
import random
from threading import Thread
from collections import defaultdict


class TaskGenerator:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="task_generator")
        self.results = []

        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Task Generator connected with result code {rc}")
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
            print(f"Task {data['task_id']} completed with status {data['status']}")

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by Task Generator")

    def start(self):
        """Start the task generator"""
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print("Task Generator started")

        # Start in a separate thread to allow for interactive commands
        self.mqtt_client.loop_start()

    def generate_task_set(self, set_type, client_id, count=5):
        """Generate a set of tasks with specific characteristics"""
        tasks = []

        if set_type == 1:  # Harmonic periods
            periods = [2, 4, 8, 16]
            for i in range(count):
                period = random.choice(periods)
                tasks.append(
                    {
                        "task_id": f"T{self.task_counter}",
                        "client_id": client_id,
                        "computation_time": random.uniform(0.1, period / 2),
                        "period": period,
                        "deadline": period,
                        "scheduling": "RM",  # Rate Monotonic is best for harmonic tasks
                    }
                )
                self.task_counter += 1

        elif set_type == 2:  # Short deadlines
            for i in range(count):
                tasks.append(
                    {
                        "task_id": f"T{self.task_counter}",
                        "client_id": client_id,
                        "computation_time": random.uniform(0.5, 2),
                        "period": random.uniform(5, 10),
                        "deadline": random.uniform(1, 3),  # Tight deadlines
                        "scheduling": "EDF",  # EDF is best for tight deadlines
                    }
                )
                self.task_counter += 1

        elif set_type == 3:  # Equal periods and computation times
            period = 5
            comp_time = 1
            for i in range(count):
                tasks.append(
                    {
                        "task_id": f"T{self.task_counter}",
                        "client_id": client_id,
                        "computation_time": comp_time,
                        "period": period,
                        "deadline": period,
                        "scheduling": "RR",  # Round Robin is fair for equal tasks
                    }
                )
                self.task_counter += 1

        return tasks

    def submit_tasks(self, task_set):
        """Submit tasks to the broker"""
        scheduling = task_set.get("scheduling", "RM")
        tasks = task_set.get("tasks", [])

        for task in tasks:
            task_data = {
                "scheduling": scheduling,
                "tasks": [task]
            }
            self.mqtt_client.publish("task/submit", json.dumps(task_data))
            
            # Enhanced logging based on task type
            load_info = ""
            if "load_type" in task:
                load_info = f" ({task['load_type']} load)"
            elif "priority" in task:
                load_info = f" ({task['priority']} priority)"
            elif "burst_type" in task:
                load_info = f" ({task['burst_type']} burst)"
                
            print(f"Submitted task {task['task_id']}{load_info} with {scheduling} scheduling")
            time.sleep(0.5)  # Space out task submissions

    def get_results(self):
        """Get the task execution results"""
        return self.results

    def analyze_results(self):
        """Analyze the task execution results with load balancing metrics"""
        if not self.results:
            print("No results to analyze")
            return

        total_tasks = len(self.results)
        completed = sum(1 for r in self.results if r["status"] == "completed")
        missed = total_tasks - completed
        avg_response = sum(r["response_time"] for r in self.results) / total_tasks

        print("\n=== Results Analysis ===")
        print(f"Total tasks: {total_tasks}")
        print(f"Completed on time: {completed} ({completed/total_tasks*100:.1f}%)")
        print(f"Missed deadlines: {missed} ({missed/total_tasks*100:.1f}%)")
        print(f"Average response time: {avg_response:.2f} seconds")

        # Group by algorithm and analyze load distribution
        algorithms = {}
        for r in self.results:
            alg = r.get("algorithm", "unknown")
            if alg not in algorithms:
                algorithms[alg] = {
                    "tasks": [],
                    "execution_times": [],
                    "response_times": [],
                    "load_types": defaultdict(int)
                }
            algorithms[alg]["tasks"].append(r)
            algorithms[alg]["execution_times"].append(r.get("actual_execution_time", 0))
            algorithms[alg]["response_times"].append(r["response_time"])
            
            # Track load types if available
            if "load_type" in r:
                algorithms[alg]["load_types"][r["load_type"]] += 1
            elif "priority" in r:
                algorithms[alg]["load_types"][r["priority"]] += 1
            elif "burst_type" in r:
                algorithms[alg]["load_types"][r["burst_type"]] += 1

        # Print detailed analysis for each algorithm
        for alg, data in algorithms.items():
            tasks = data["tasks"]
            total = len(tasks)
            completed = sum(1 for t in tasks if t["status"] == "completed")
            
            print(f"\nAlgorithm {alg}:")
            print(f"  Tasks: {total}")
            print(f"  Completion rate: {completed/total*100:.1f}%")
            print(f"  Avg response: {sum(data['response_times'])/total:.2f}s")
            print(f"  Avg execution: {sum(data['execution_times'])/total:.2f}s")
            
            # Print load distribution
            if data["load_types"]:
                print("  Load distribution:")
                for load_type, count in data["load_types"].items():
                    print(f"    {load_type}: {count} tasks ({count/total*100:.1f}%)")

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

    def get_task_set_type_name(self, set_type):
        """Convert set type number to corresponding JSON key"""
        type_mapping = {
            1: "harmonic_sets",
            2: "tight_deadline_sets",
            3: "equal_period_sets"
        }
        return type_mapping.get(set_type)


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
            print("2. Varying Priority")
            print("3. Burst")

            set_type = input("Enter task set type: ")
            if set_type not in ["1", "2", "3"]:
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
