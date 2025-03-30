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
        self.task_counter = 0

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
            print(f"Task {data['task_id']} completed with status {data['status']}")

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by Task Generator")

    def start(self):
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print("Task Generator started")
        self.mqtt_client.loop_start()

    def generate_task_set(self, set_type, client_id, count=5, scheduling="RM"):
        tasks = []
        if set_type == 1:  # Mixed Utilization
            for i in range(count):
                if i < count // 3:
                    load_type = "light"
                    execution_time = 0.8
                elif i < 2 * count // 3:
                    load_type = "medium"
                    execution_time = 3.0
                else:
                    load_type = "heavy"
                    execution_time = 6.4
                tasks.append({
                    "task_id": f"T{self.task_counter}",
                    "client_id": client_id,
                    "execution_time": execution_time,
                    "deadline": execution_time * 3,
                    "scheduling": scheduling,
                    "load_type": load_type
                })
                self.task_counter += 1
        elif set_type == 2:  # Varying Priority
            for i in range(count):
                if i < count // 3:
                    priority = "high"
                    execution_time = 0.6
                    deadline = 2.0
                elif i < 2 * count // 3:
                    priority = "medium"
                    execution_time = 2.0
                    deadline = 6.0
                else:
                    priority = "low"
                    execution_time = 4.0
                    deadline = 14.0
                tasks.append({
                    "task_id": f"T{self.task_counter}",
                    "client_id": client_id,
                    "execution_time": execution_time,
                    "deadline": deadline,
                    "scheduling": scheduling,
                    "priority": priority
                })
                self.task_counter += 1
        elif set_type == 3:  # Burst
            for i in range(count):
                if i < count // 3:
                    burst_type = "rapid"
                    execution_time = 0.5
                    deadline = 2.0
                elif i < 2 * count // 3:
                    burst_type = "medium"
                    execution_time = 2.0
                    deadline = 7.0
                else:
                    burst_type = "heavy"
                    execution_time = 4.0
                    deadline = 14.0
                tasks.append({
                    "task_id": f"T{self.task_counter}",
                    "client_id": client_id,
                    "execution_time": execution_time,
                    "deadline": deadline,
                    "scheduling": scheduling,
                    "burst_type": burst_type
                })
                self.task_counter += 1
        return {"scheduling": scheduling, "tasks": tasks, "batch_submission": True, "timestamp": time.time()}

    def submit_tasks(self, task_set):
        self.mqtt_client.publish("task/submit", json.dumps(task_set))
        print(f"\n[BATCH SUBMISSION] Sent {len(task_set['tasks'])} tasks with {task_set['scheduling']} scheduling")

    def get_results(self):
        return self.results

    def analyze_results(self):
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

if __name__ == "__main__":
    generator = TaskGenerator()
    generator.start()

    while True:
        print("\nScheduling Options:")
        print("1. Rate Monotonic (RM)")
        print("2. Earliest Deadline First (EDF)")
        print("3. Round Robin (RR)")
        print("4. Critical Ratio Scheduling (CRS)")
        print("5. Highest Response Ratio Next (HRRN)")
        print("6. Analyze results")
        print("7. Exit")
        choice = input("Enter scheduling choice: ").strip()
        
        if choice == "7":
            break
        elif choice == "6":
            generator.analyze_results()
            continue
        
        if choice == "1":
            scheduling = "RM"
        elif choice == "2":
            scheduling = "EDF"
        elif choice == "3":
            scheduling = "RR"
        elif choice == "4":
            scheduling = "CRS"
        elif choice == "5":
            scheduling = "HRRN"
        else:
            print("Invalid choice. Try again.")
            continue
        
        print("\nTask Set Types:")
        print("1. Mixed Utilization")
        print("2. Varying Priority")
        print("3. Burst")
        set_choice = input("Enter task set type: ").strip()
        if set_choice not in ["1", "2", "3"]:
            print("Invalid task set type. Try again.")
            continue
        set_type = int(set_choice)
        
        task_set = generator.generate_task_set(set_type, "test_client_1", count=18, scheduling=scheduling)
        generator.submit_tasks(task_set)
