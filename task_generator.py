# task_generator.py
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
        # Get the scheduling algorithm from the task set
        scheduling = task_set.get("scheduling", "RM")
        tasks = task_set.get("tasks", [])

        for task in tasks:
            task_data = {
                "scheduling": scheduling,
                "tasks": [task]  # Send one task at a time
            }
            self.mqtt_client.publish("task/submit", json.dumps(task_data))
            print(f"Submitted task {task['task_id']} with {scheduling} scheduling")
            time.sleep(0.5)  # Space out task submissions

    def get_results(self):
        """Get the task execution results"""
        return self.results

    def analyze_results(self):
        """Analyze the task execution results"""
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

        # Group by algorithm if available
        algorithms = {}
        for r in self.results:
            alg = r.get("algorithm", "unknown")
            if alg not in algorithms:
                algorithms[alg] = []
            algorithms[alg].append(r)

        for alg, tasks in algorithms.items():
            total = len(tasks)
            completed = sum(1 for t in tasks if t["status"] == "completed")
            print(f"\nAlgorithm {alg}:")
            print(f"  Tasks: {total}")
            print(f"  Completion rate: {completed/total*100:.1f}%")
            print(
                f"  Avg response: {sum(t['response_time'] for t in tasks)/total:.2f}s"
            )

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
            print("\nOptions:")
            print("1. Submit harmonic task set (RM)")
            print("2. Submit tight deadline task set (EDF)")
            print("3. Submit equal period task set (RR)")
            print("4. Analyze results")
            print("5. Exit")

            choice = input("Enter choice: ")

            task_sets = generator.load_task_sets()
            if not task_sets:
                print("No task sets found")
                continue

            if choice == "1":
                print("\nAvailable harmonic task sets:")
                for i, task_set in enumerate(task_sets["harmonic_sets"]):
                    print(f"{i + 1}. Task set with {len(task_set['tasks'])} tasks ({task_set['scheduling']})")
                
                set_choice = input("Select task set number: ")
                try:
                    set_index = int(set_choice) - 1
                    if 0 <= set_index < len(task_sets["harmonic_sets"]):
                        generator.submit_tasks(task_sets["harmonic_sets"][set_index])
                    else:
                        print("Invalid task set number")
                except ValueError:
                    print("Invalid input")

            elif choice == "2":
                print("\nAvailable tight deadline task sets:")
                for i, task_set in enumerate(task_sets["tight_deadline_sets"]):
                    print(f"{i + 1}. Task set with {len(task_set['tasks'])} tasks ({task_set['scheduling']})")
                
                set_choice = input("Select task set number: ")
                try:
                    set_index = int(set_choice) - 1
                    if 0 <= set_index < len(task_sets["tight_deadline_sets"]):
                        generator.submit_tasks(task_sets["tight_deadline_sets"][set_index])
                    else:
                        print("Invalid task set number")
                except ValueError:
                    print("Invalid input")

            elif choice == "3":
                print("\nAvailable equal period task sets:")
                for i, task_set in enumerate(task_sets["equal_period_sets"]):
                    print(f"{i + 1}. Task set with {len(task_set['tasks'])} tasks ({task_set['scheduling']})")
                
                set_choice = input("Select task set number: ")
                try:
                    set_index = int(set_choice) - 1
                    if 0 <= set_index < len(task_sets["equal_period_sets"]):
                        generator.submit_tasks(task_sets["equal_period_sets"][set_index])
                    else:
                        print("Invalid task set number")
                except ValueError:
                    print("Invalid input")

            elif choice == "4":
                generator.analyze_results()
            elif choice == "5":
                break
            else:
                print("Invalid choice")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        generator.mqtt_client.loop_stop()
