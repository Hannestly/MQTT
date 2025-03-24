# mqtt_broker.py
import paho.mqtt.client as mqtt
import threading
import time
from collections import defaultdict, deque
import json
import heapq


class TaskScheduler:
    def __init__(self):
        self.tasks = []
        self.current_time = 0
        self.scheduling_algorithms = {
            "RM": self.rate_monotonic,
            "EDF": self.earliest_deadline_first,
            "RR": self.round_robin,
        }
        self.time_quantum = 1  # For Round-Robin
        self.task_queue = deque()
        self.last_scheduled = None
        self.schedule_history = []

    def add_task(self, task):
        """Add a new task to the scheduler"""
        if task["scheduling"] == "RM":
            # For RM, priority is based on period (shorter period = higher priority)
            priority = task["period"]
        elif task["scheduling"] == "EDF":
            # For EDF, priority is based on absolute deadline
            priority = task["arrival_time"] + task["deadline"]
        else:
            # For RR, priority isn't used
            priority = 0

        heapq.heappush(self.tasks, (priority, task))

    def rate_monotonic(self):
        """Rate Monotonic scheduling algorithm"""
        if not self.tasks:
            return None

        # Get the task with the highest priority (smallest period)
        _, task = heapq.heappop(self.tasks)
        return task

    def earliest_deadline_first(self):
        """Earliest Deadline First scheduling algorithm"""
        if not self.tasks:
            return None

        # Get the task with the earliest deadline
        _, task = heapq.heappop(self.tasks)
        return task

    def round_robin(self):
        """Round Robin scheduling algorithm"""
        if not self.tasks and not self.task_queue:
            return None

        # If no task is currently executing or time quantum expired
        if (
            not self.last_scheduled
            or (time.time() - self.last_scheduled["start_time"]) >= self.time_quantum
        ):
            if self.tasks:
                # Get the next task from priority queue
                _, task = heapq.heappop(self.tasks)
                task["start_time"] = time.time()
                self.last_scheduled = task
                return task
            elif self.task_queue:
                # Get the next task from the queue
                task = self.task_queue.popleft()
                task["start_time"] = time.time()
                self.last_scheduled = task
                return task
        else:
            # Continue executing the current task
            return self.last_scheduled

    def schedule(self, algorithm):
        """Schedule tasks using the specified algorithm"""
        if algorithm not in self.scheduling_algorithms:
            raise ValueError(f"Unknown scheduling algorithm: {algorithm}")

        scheduled_task = self.scheduling_algorithms[algorithm]()
        if scheduled_task:
            self.schedule_history.append(
                {"time": time.time(), "task": scheduled_task, "algorithm": algorithm}
            )
        return scheduled_task

    def get_schedule_history(self):
        """Get the scheduling history"""
        return self.schedule_history


class MQTTBroker:
    def __init__(self, host="localhost", port=1883):
        self.host = host
        self.port = port
        self.client = mqtt.Client()
        self.clients = set()
        self.task_scheduler = TaskScheduler()
        self.task_status = defaultdict(dict)

        # Setup MQTT callbacks
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Broker connected with result code {rc}")
        client.subscribe("client/register")
        client.subscribe("task/submit")
        client.subscribe("task/status")
        client.subscribe("system/control")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic == "client/register":
            self.handle_client_registration(data)
        elif topic == "task/submit":
            self.handle_task_submission(data)
        elif topic == "task/status":
            self.handle_task_status(data)
        elif topic == "system/control":
            self.handle_system_control(data)

    def handle_client_registration(self, data):
        """Handle new client registration"""
        client_id = data.get("client_id")
        if client_id:
            self.clients.add(client_id)
            print(f"Client {client_id} registered")
            self.client.publish(
                f"client/{client_id}/registered", json.dumps({"status": "success"})
            )

    def handle_task_submission(self, data):
        """Handle new task submission"""
        task_id = data.get("task_id")
        client_id = data.get("client_id")
        scheduling = data.get("scheduling", "RM")  # Default to Rate Monotonic

        if not task_id or not client_id:
            print("Invalid task submission - missing task_id or client_id")
            return

        # Add task to scheduler
        task = {
            "task_id": task_id,
            "client_id": client_id,
            "arrival_time": time.time(),
            "computation_time": data.get("computation_time", 1),
            "period": data.get("period", 5),
            "deadline": data.get("deadline", data.get("period", 5)),
            "scheduling": scheduling,
        }

        self.task_scheduler.add_task(task)
        print(f"Task {task_id} from client {client_id} added to scheduler")

        # Acknowledge task submission
        self.client.publish(
            f"task/{task_id}/ack",
            json.dumps(
                {"task_id": task_id, "status": "scheduled", "timestamp": time.time()}
            ),
        )

    def handle_task_status(self, data):
        """Handle task status updates"""
        task_id = data.get("task_id")
        client_id = data.get("client_id")
        status = data.get("status")

        if task_id and client_id and status:
            self.task_status[task_id] = {
                "client_id": client_id,
                "status": status,
                "timestamp": time.time(),
            }
            print(f"Task {task_id} status updated to {status}")

    def handle_system_control(self, data):
        """Handle system control commands"""
        command = data.get("command")

        if command == "get_schedule_history":
            history = self.task_scheduler.get_schedule_history()
            self.client.publish("system/schedule_history", json.dumps(history))
        elif command == "get_task_status":
            task_id = data.get("task_id")
            if task_id in self.task_status:
                self.client.publish(
                    f"task/{task_id}/status", json.dumps(self.task_status[task_id])
                )
            else:
                self.client.publish(
                    f"task/{task_id}/status",
                    json.dumps({"status": "unknown", "timestamp": time.time()}),
                )

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f"Subscribed to topic with QoS: {granted_qos}")

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published")

    def start(self):
        """Start the MQTT broker"""
        self.client.connect(self.host, self.port)
        print(f"MQTT Broker started on {self.host}:{self.port}")

        # Start scheduling loop in a separate thread
        scheduler_thread = threading.Thread(target=self.run_scheduler)
        scheduler_thread.daemon = True
        scheduler_thread.start()

        self.client.loop_forever()

    def run_scheduler(self):
        """Continuous scheduling loop"""
        while True:
            # Check for tasks to schedule
            for client_id in self.clients:
                # Get the next task for each scheduling algorithm
                for algorithm in ["RM", "EDF", "RR"]:
                    task = self.task_scheduler.schedule(algorithm)
                    if task and task["client_id"] == client_id:
                        # Send task to client
                        self.client.publish(
                            f"client/{client_id}/task",
                            json.dumps(
                                {
                                    "task_id": task["task_id"],
                                    "computation_time": task["computation_time"],
                                    "deadline": task["deadline"],
                                    "algorithm": algorithm,
                                    "timestamp": time.time(),
                                }
                            ),
                        )
            time.sleep(0.1)  # Prevent busy waiting


if __name__ == "__main__":
    broker = MQTTBroker()
    broker.start()
