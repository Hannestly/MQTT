# embedded_client.py
import paho.mqtt.client as mqtt
import time
import json
import threading
from collections import deque
import random


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
        self.current_load = 0.0
        self.last_heartbeat = time.time()
        self.heartbeat_interval = 1.0
        self.is_registered = False
        self.is_connected = False  # Add connection flag
        
        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_publish = self.on_publish
        self.mqtt_client.on_disconnect = self.on_disconnect

    def on_connect(self, client, userdata, flags, rc):
        print(f"Worker {self.client_id} connected with result code {rc}")
        self.is_connected = True  # Set connection flag
        # Subscribe to topics
        client.subscribe(f"worker/{self.client_id}/task")
        client.subscribe("system/control")
        client.subscribe(f"client/{self.client_id}/registered")

        # Send initial registration
        self.register_with_broker()

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic == f"client/{self.client_id}/registered":
            if data.get("status") == "success":
                print(f"Worker {self.client_id} successfully registered with broker")
                self.is_registered = True
        elif topic == f"worker/{self.client_id}/task":
            self.handle_task_assignment(data)
        elif topic == "system/control":
            self.handle_system_control(data)

    def handle_task_assignment(self, data):
        """Handle new task assignment from broker"""
        task_id = data.get("task_id")
        execution_time = data.get("execution_time", 1)
        deadline = data.get("deadline", 5)
        algorithm = data.get("algorithm")

        print(f"\n{self.client_id} received task {task_id}:")
        print(f"  Execution time: {execution_time}")
        print(f"  Deadline: {deadline}")
        print(f"  Algorithm: {algorithm}")

        # Add task to queue
        task = {
            "task_id": task_id,
            "execution_time": execution_time,
            "deadline": deadline,
            "algorithm": algorithm,
            "arrival_time": time.time(),
            "assigned_time": data.get("timestamp", time.time()),
            "load_type": data.get("load_type"),
            "priority": data.get("priority"),
            "burst_type": data.get("burst_type")
        }

        self.task_queue.append(task)
        self.update_current_load()
        print(f"{self.client_id}: Task {task_id} added to queue. Queue size: {len(self.task_queue)}")

    def determine_task_set_type(self, task_data):
        """Determine the task set type based on task characteristics"""
        if "load_type" in task_data:
            return "mixed_utilization"
        elif "priority" in task_data:
            return "varying_priority"
        elif "burst_type" in task_data:
            return "burst"
        return "unknown"

    def update_current_load(self):
        """Update current load based on queue and executing task"""
        queue_load = sum(task["execution_time"] for task in self.task_queue)
        executing_load = (
            self.current_task["execution_time"] if self.current_task else 0
        )
        self.current_load = queue_load + executing_load
        self.send_heartbeat()

    def send_heartbeat(self):
        """Send heartbeat with current load information"""
        if not self.is_connected:
            print(f"{self.client_id}: Not connected, can't send heartbeat")
            return

        current_time = time.time()
        if current_time - self.last_heartbeat >= self.heartbeat_interval:
            try:
                heartbeat_msg = {
                    "worker_id": self.client_id,
                    "timestamp": current_time,
                    "current_load": self.current_load,
                    "queue_size": len(self.task_queue),
                    "is_busy": self.current_task is not None
                }
                result = self.mqtt_client.publish(
                    "worker/heartbeat",
                    json.dumps(heartbeat_msg)
                )
                if result.rc == mqtt.MQTT_ERR_SUCCESS:
                    self.last_heartbeat = current_time
                    print(f"{self.client_id}: Heartbeat sent successfully")
                else:
                    print(f"{self.client_id}: Failed to send heartbeat, rc={result.rc}")
            except Exception as e:
                print(f"{self.client_id}: Error sending heartbeat: {e}")

    def handle_system_control(self, data):
        """Handle system control commands"""
        command = data.get("command")

        if command == "get_task_history":
            self.mqtt_client.publish(
                f"client/{self.client_id}/task_history", json.dumps(self.task_history)
            )

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published by {self.client_id}")

    def on_disconnect(self, client, userdata, rc):
        print(f"Worker {self.client_id} disconnected with result code {rc}")
        self.is_connected = False

    def mqtt_loop(self):
        """Run MQTT client loop in separate thread"""
        try:
            print(f"{self.client_id}: Starting MQTT loop")
            self.mqtt_client.loop_forever()
        except Exception as e:
            print(f"{self.client_id}: MQTT loop error: {e}")

    def start(self):
        """Start the client"""
        try:
            # Connect to broker
            print(f"{self.client_id}: Connecting to broker at {self.broker_host}:{self.broker_port}")
            self.mqtt_client.connect(self.broker_host, self.broker_port)

            # Start MQTT loop in a separate thread
            mqtt_thread = threading.Thread(target=self.mqtt_loop)
            mqtt_thread.daemon = True
            mqtt_thread.start()

            # Start task processing in a separate thread
            processor_thread = threading.Thread(target=self.process_tasks)
            processor_thread.daemon = True
            processor_thread.start()

            # Main loop for heartbeat
            while True:
                current_time = time.time()
                if current_time - self.last_heartbeat >= 1.0:  # Send heartbeat every second
                    heartbeat_msg = {
                        "worker_id": self.client_id,
                        "timestamp": current_time,
                        "current_load": self.current_load,
                        "queue_size": len(self.task_queue),
                        "is_busy": self.current_task is not None
                    }
                    print(f"{self.client_id}: Sending heartbeat")  # Debug print
                    self.mqtt_client.publish("worker/heartbeat", json.dumps(heartbeat_msg))
                    self.last_heartbeat = current_time

                time.sleep(0.1)  # Small sleep to prevent CPU overuse

        except Exception as e:
            print(f"{self.client_id}: Error in main loop: {e}")

    def process_tasks(self):
        """Process tasks from the queue"""
        while True:
            if self.task_queue and not self.current_task:
                self.current_task = self.task_queue.popleft()
                self.task_start_time = time.time()

                print(f"\n{self.client_id} starting task {self.current_task['task_id']}")

                # Calculate execution time with random delay
                base_execution_time = self.current_task["execution_time"]
                random_delay = random.uniform(0, 1)
                total_execution_time = base_execution_time + random_delay

                print(f"{self.client_id}: Executing task {self.current_task['task_id']} "
                      f"for {total_execution_time:.2f}s")
                
                # Simulate task execution
                time.sleep(total_execution_time)

                # Task completed
                completion_time = time.time()
                response_time = completion_time - self.current_task["assigned_time"]
                deadline_met = completion_time <= (
                    self.current_task["arrival_time"] + self.current_task["deadline"]
                )

                status_msg = {
                    "task_id": self.current_task["task_id"],
                    "worker_id": self.client_id,
                    "status": "completed" if deadline_met else "missed_deadline",
                    "response_time": response_time,
                    "actual_execution_time": total_execution_time,
                    "base_execution_time": base_execution_time,
                    "random_delay": random_delay,
                    "current_load": self.current_load,
                    "queue_size": len(self.task_queue),
                    "algorithm": self.current_task["algorithm"],
                    "timestamp": completion_time,
                }

                print(f"{self.client_id}: Task {self.current_task['task_id']} completed")
                self.mqtt_client.publish("task/status", json.dumps(status_msg))

                self.task_history.append(status_msg)
                self.current_task = None
                self.task_start_time = None

            time.sleep(0.1)

    def register_with_broker(self):
        """Register with the broker"""
        if not self.is_registered:
            registration_data = {
                "client_id": self.client_id,
                "timestamp": time.time(),
                "current_load": self.current_load
            }
            print(f"Attempting to register {self.client_id} with broker...")
            self.mqtt_client.publish("client/register", json.dumps(registration_data))

    def registration_retry_loop(self):
        """Periodically retry registration until successful"""
        while True:
            if not self.is_registered:
                self.register_with_broker()
            time.sleep(5)  # Retry every 5 seconds


def start_worker(worker_id, broker_host, broker_port):
    """Start a single worker"""
    worker = EmbeddedClient(worker_id, broker_host, broker_port)
    try:
        worker.start()
    except Exception as e:
        print(f"Error in {worker_id}: {e}")

if __name__ == "__main__":
    import sys
    from multiprocessing import Process

    # Default settings
    broker_host = "localhost"
    broker_port = 1883
    
    # Override defaults if provided
    if len(sys.argv) > 1:
        broker_host = sys.argv[1]
    if len(sys.argv) > 2:
        broker_port = int(sys.argv[2])

    # Create workers
    workers = []
    for i in range(1, 4):  # Create workers 1-3
        worker_id = f"worker_{i}"
        print(f"Starting {worker_id}...")
        
        # Create each worker in a separate process
        worker_process = Process(
            target=start_worker,
            args=(worker_id, broker_host, broker_port)
        )
        worker_process.daemon = True
        workers.append(worker_process)
        worker_process.start()

    try:
        # Keep the main process running
        while True:
            time.sleep(1)
            # Check if all workers are alive
            for i, worker in enumerate(workers):
                if not worker.is_alive():
                    print(f"Worker_{i+1} died, restarting...")
                    # Restart the dead worker
                    worker_id = f"worker_{i+1}"
                    new_worker = Process(
                        target=start_worker,
                        args=(worker_id, broker_host, broker_port)
                    )
                    new_worker.daemon = True
                    workers[i] = new_worker
                    new_worker.start()

    except KeyboardInterrupt:
        print("\nShutting down workers...")
        for worker in workers:
            worker.terminate()
            worker.join()
        print("All workers stopped")
