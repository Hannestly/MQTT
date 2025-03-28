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
        self.worker_loads = {
            "worker_1": 0.0,
            "worker_2": 0.0,
            "worker_3": 0.0
        }

    def get_least_loaded_worker(self):
        """Return the worker with the lowest current load"""
        return min(self.worker_loads.items(), key=lambda x: x[1])[0]

    def update_worker_load(self, worker_id, task_load):
        """Update worker's load based on task execution time"""
        self.worker_loads[worker_id] = task_load

    def add_task(self, task):
        """Add a new task to the scheduler with load balancing consideration"""
        if task["scheduling"] == "RM":
            # For RM, priority is based on period (shorter period = higher priority)
            priority = task["period"]
        elif task["scheduling"] == "EDF":
            # For EDF, priority is based on absolute deadline
            priority = task["arrival_time"] + task["deadline"]
        else:
            # For RR, priority isn't used
            priority = 0

        # Add load balancing metadata
        task["assigned_worker"] = self.get_least_loaded_worker()
        heapq.heappush(self.tasks, (priority, task))

    def rate_monotonic(self):
        """Rate Monotonic scheduling algorithm"""
        if not self.tasks:
            return None

        # Find task with shortest period (highest priority)
        current_time = time.time()
        valid_tasks = []
        
        # Check all tasks and keep only those that are ready to run
        while self.tasks:
            priority, task = heapq.heappop(self.tasks)
            if current_time >= task["arrival_time"]:
                valid_tasks.append((priority, task))
                
        if not valid_tasks:
            # Push back tasks and return None if no valid tasks
            for task in valid_tasks:
                heapq.heappush(self.tasks, task)
            return None
            
        # Get task with shortest period
        selected_task = min(valid_tasks, key=lambda x: x[1]["period"])
        
        # Push back unselected tasks
        for task in valid_tasks:
            if task != selected_task:
                heapq.heappush(self.tasks, task)
                
        return selected_task[1]

    def earliest_deadline_first(self):
        """Earliest Deadline First scheduling algorithm"""
        if not self.tasks:
            return None

        # Find task with earliest absolute deadline
        current_time = time.time()
        valid_tasks = []
        
        # Check all tasks and keep only those that are ready to run
        while self.tasks:
            priority, task = heapq.heappop(self.tasks)
            if current_time >= task["arrival_time"]:
                # Calculate absolute deadline
                abs_deadline = task["arrival_time"] + task["deadline"]
                if current_time <= abs_deadline:
                    valid_tasks.append((priority, task))
                    
        if not valid_tasks:
            # Push back tasks and return None if no valid tasks
            for task in valid_tasks:
                heapq.heappush(self.tasks, task)
            return None
            
        # Get task with earliest deadline
        selected_task = min(valid_tasks, key=lambda x: x[1]["arrival_time"] + x[1]["deadline"])
        
        # Push back unselected tasks
        for task in valid_tasks:
            if task != selected_task:
                heapq.heappush(self.tasks, task)
                
        return selected_task[1]

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
        self.worker_status = {
            "worker_1": {"active": False, "last_seen": 0, "current_load": 0.0, "heartbeat_count": 0},
            "worker_2": {"active": False, "last_seen": 0, "current_load": 0.0, "heartbeat_count": 0},
            "worker_3": {"active": False, "last_seen": 0, "current_load": 0.0, "heartbeat_count": 0}
        }
        self.worker_timeout = 5.0  # Time in seconds before marking worker as inactive

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
        client.subscribe("worker/heartbeat")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        try:
            if topic == "worker/heartbeat":
                data = json.loads(msg.payload.decode())
                worker_id = data.get("worker_id")
                if worker_id in self.worker_status:
                    self.worker_status[worker_id]["last_seen"] = time.time()
                    self.worker_status[worker_id]["current_load"] = data.get("current_load", 0.0)
                    self.worker_status[worker_id]["heartbeat_count"] += 1
                    if self.worker_status[worker_id]["heartbeat_count"] % 5 == 0:  # Log every 5th heartbeat
                        print(f"[HEARTBEAT] {worker_id}: Count={self.worker_status[worker_id]['heartbeat_count']}")
            elif topic == "client/register":
                self.handle_client_registration(json.loads(msg.payload.decode()))
            elif topic == "task/submit":
                self.handle_task_submission(json.loads(msg.payload.decode()))
            elif topic == "task/status":
                self.handle_task_status(json.loads(msg.payload.decode()))
            elif topic == "system/control":
                self.handle_system_control(json.loads(msg.payload.decode()))
        except Exception as e:
            print(f"Error processing message on {topic}: {e}")

    def handle_client_registration(self, data):
        """Handle new client (worker) registration"""
        client_id = data.get("client_id")
        if client_id in self.worker_status:
            self.worker_status[client_id]["active"] = True
            self.worker_status[client_id]["last_seen"] = time.time()
            self.worker_status[client_id]["current_load"] = data.get("current_load", 0.0)
            self.worker_status[client_id]["heartbeat_count"] = 0  # Add heartbeat counter
            print(f"\n[WORKER REGISTERED] {client_id}")
            print(f"  Current load: {self.worker_status[client_id]['current_load']}")
            
            self.client.publish(
                f"client/{client_id}/registered",
                json.dumps({
                    "status": "success",
                    "timestamp": time.time()
                })
            )
            
            active_workers = [w_id for w_id, status in self.worker_status.items() 
                             if status["active"]]
            print(f"  Active workers: {active_workers}")

    def handle_task_submission(self, data):
        """Handle new task submission"""
        try:
            if isinstance(data, str):
                data = json.loads(data)
            
            # Extract scheduling algorithm from the task set
            scheduling = data.get("scheduling")
            tasks = data.get("tasks", [])

            if not tasks or not scheduling:
                print("Invalid task submission - missing tasks or scheduling algorithm")
                return

            # Process each task in the task set
            for task in tasks:
                task_id = task.get("task_id")
                if not task_id:
                    print("Invalid task - missing task_id")
                    continue

                # Create task object with the new structure
                scheduled_task = {
                    "task_id": task_id,
                    "arrival_time": time.time(),
                    "computation_time": task.get("execution_time", 1),
                    "period": task.get("period", 5),
                    "deadline": task.get("deadline", task.get("period", 5)),
                    "scheduling": scheduling,
                    "load_type": task.get("load_type"),
                    "priority": task.get("priority"),
                    "burst_type": task.get("burst_type")
                }

                print(f"\n[TASK ARRIVED] Task {task_id}")
                print(f"  Scheduling: {scheduling}")
                print(f"  Execution Time: {scheduled_task['computation_time']}")
                print(f"  Deadline: {scheduled_task['deadline']}")

                self.task_scheduler.add_task(scheduled_task)

        except Exception as e:
            print(f"Error handling task submission: {e}")

    def handle_task_status(self, data):
        """Handle task status updates with load balancing metrics"""
        task_id = data.get("task_id")
        worker_id = data.get("worker_id")
        status = data.get("status")
        execution_time = data.get("actual_execution_time", 0)

        if task_id and worker_id and status:
            print(f"\n[TASK COMPLETED] Task {task_id}")
            print(f"  Worker: {worker_id}")
            print(f"  Status: {status}")
            print(f"  Execution Time: {execution_time:.2f}s")
            if status != "completed":
                print(f"  Note: Deadline missed!")

            # Update task status
            self.task_status[task_id] = {
                "worker_id": worker_id,
                "status": status,
                "execution_time": execution_time,
                "timestamp": time.time(),
            }

            # Update worker load
            if worker_id in self.worker_status:
                self.worker_status[worker_id]["last_seen"] = time.time()
                self.worker_status[worker_id]["current_load"] = data.get("current_load", 0.0)
                self.task_scheduler.update_worker_load(worker_id, data.get("current_load", 0.0))

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
        """Continuous scheduling loop with load balancing"""
        while True:
            current_time = time.time()
            
            # Monitor worker health
            for worker_id, status in self.worker_status.items():
                if status["active"]:
                    time_since_last_seen = current_time - status["last_seen"]
                    if time_since_last_seen > self.worker_timeout:
                        status["active"] = False
                        print(f"\n[WORKER STATUS] Worker {worker_id} marked as inactive")
                        print(f"  Last seen: {time_since_last_seen:.1f} seconds ago")

            # Process tasks only if there are active workers
            active_workers = [w_id for w_id, status in self.worker_status.items() 
                            if status["active"]]
            
            if self.task_scheduler.tasks:
                if not active_workers:
                    print("\n[SCHEDULER WARNING] No active workers available")
                    time.sleep(1)  # Wait a bit before checking again
                    continue
                
                # Print active workers when there are tasks to schedule
                print(f"\n[SCHEDULER STATUS]")
                print(f"  Active workers: {active_workers}")
                print(f"  Tasks waiting: {len(self.task_scheduler.tasks)}")

                # Get scheduling algorithm from the first task
                _, current_task = self.task_scheduler.tasks[0]
                algorithm = current_task.get("scheduling")
                
                if algorithm in self.task_scheduler.scheduling_algorithms:
                    task = self.task_scheduler.schedule(algorithm)
                    if task:
                        worker_id = task["assigned_worker"]
                        if self.worker_status[worker_id]["active"]:
                            print(f"\n[TASK ASSIGNED] Task {task['task_id']}")
                            print(f"  Worker: {worker_id}")
                            print(f"  Algorithm: {algorithm}")
                            print(f"  Execution Time: {task['computation_time']}s")
                            
                            # Send task to assigned worker
                            self.client.publish(
                                f"worker/{worker_id}/task",
                                json.dumps({
                                    "task_id": task["task_id"],
                                    "execution_time": task["computation_time"],
                                    "deadline": task["deadline"],
                                    "algorithm": task["scheduling"],
                                    "load_type": task.get("load_type", "unknown"),
                                    "priority": task.get("priority", "unknown"),
                                    "burst_type": task.get("burst_type", "unknown"),
                                    "timestamp": time.time()
                                })
                            )
                        else:
                            print(f"\n[SCHEDULER WARNING] Selected worker {worker_id} is not active")
                    else:
                        print(f"\n[SCHEDULER WARNING] No task selected by scheduler")

            time.sleep(0.1)

    def get_system_status(self):
        """Get current system status including load balancing metrics"""
        return {
            "workers": self.worker_status,
            "tasks_pending": len(self.task_scheduler.tasks),
            "tasks_completed": len([t for t in self.task_status.values() 
                                  if t["status"] == "completed"]),
            "load_distribution": {
                worker_id: status["current_load"]
                for worker_id, status in self.worker_status.items()
                if status["active"]
            }
        }


if __name__ == "__main__":
    broker = MQTTBroker()
    broker.start()
