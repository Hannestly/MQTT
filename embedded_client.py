# embedded_client.py
import paho.mqtt.client as mqtt
import time
import json
import threading
from collections import deque


class EmbeddedClient:
    def __init__(self, client_id, broker_host="localhost", broker_port=1883):
        self.client_id = client_id
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id=client_id)
        self.task_queue = deque()
        self.current_task = None
        self.task_start_time = None
        self.task_history = []

        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Client {self.client_id} connected with result code {rc}")
        # Subscribe to relevant topics
        client.subscribe(f"client/{self.client_id}/task")
        client.subscribe("system/control")

        # Register with the broker
        client.publish(
            "client/register",
            json.dumps({"client_id": self.client_id, "timestamp": time.time()}),
        )

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic == f"client/{self.client_id}/task":
            self.handle_task_assignment(data)
        elif topic == "system/control":
            self.handle_system_control(data)

    def handle_task_assignment(self, data):
        """Handle new task assignment from broker"""
        task_id = data.get("task_id")
        computation_time = data.get("computation_time", 1)
        deadline = data.get("deadline", 5)
        algorithm = data.get("algorithm", "RM")

        print(
            f"New task received: {task_id} (CT: {computation_time}, DL: {deadline}, ALG: {algorithm})"
        )

        # Add task to queue
        task = {
            "task_id": task_id,
            "computation_time": computation_time,
            "deadline": deadline,
            "algorithm": algorithm,
            "arrival_time": time.time(),
            "assigned_time": data.get("timestamp", time.time()),
        }

        self.task_queue.append(task)

    def handle_system_control(self, data):
        """Handle system control commands"""
        command = data.get("command")

        if command == "get_task_history":
            self.mqtt_client.publish(
                f"client/{self.client_id}/task_history", json.dumps(self.task_history)
            )

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by {self.client_id}")

    def start(self):
        """Start the client"""
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print(f"Embedded Client {self.client_id} started")

        # Start task processing loop in a separate thread
        processor_thread = threading.Thread(target=self.process_tasks)
        processor_thread.daemon = True
        processor_thread.start()

        self.mqtt_client.loop_forever()

    def process_tasks(self):
        """Process tasks from the queue"""
        while True:
            if self.task_queue and not self.current_task:
                # Get the next task
                self.current_task = self.task_queue.popleft()
                self.task_start_time = time.time()

                print(f"Starting task {self.current_task['task_id']}")

                # Simulate task execution
                time.sleep(self.current_task["computation_time"])

                # Task completed
                completion_time = time.time()
                response_time = completion_time - self.current_task["assigned_time"]
                deadline_met = completion_time <= (
                    self.current_task["arrival_time"] + self.current_task["deadline"]
                )

                # Record task history
                task_record = {
                    "task_id": self.current_task["task_id"],
                    "start_time": self.task_start_time,
                    "completion_time": completion_time,
                    "response_time": response_time,
                    "deadline_met": deadline_met,
                    "algorithm": self.current_task["algorithm"],
                }
                self.task_history.append(task_record)

                # Send status update to broker
                self.mqtt_client.publish(
                    "task/status",
                    json.dumps(
                        {
                            "task_id": self.current_task["task_id"],
                            "client_id": self.client_id,
                            "status": (
                                "completed" if deadline_met else "missed_deadline"
                            ),
                            "response_time": response_time,
                            "timestamp": completion_time,
                        }
                    ),
                )

                self.current_task = None
                self.task_start_time = None

            time.sleep(0.1)  # Prevent busy waiting


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print(
            "Usage: python embedded_client.py <client_id> [broker_host] [broker_port]"
        )
        sys.exit(1)

    client_id = sys.argv[1]
    broker_host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    broker_port = int(sys.argv[3]) if len(sys.argv) > 3 else 1883

    client = EmbeddedClient(client_id, broker_host, broker_port)
    client.start()
