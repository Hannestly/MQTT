import paho.mqtt.client as mqtt
import threading
import time
from collections import defaultdict, deque
import json
import heapq
import uuid
import logging

class TaskScheduler:
    def __init__(self):
        self.tasks = []
        self.current_time = 0
        self.scheduling_algorithms = {
            "RM": self.rate_monotonic,
            "EDF": self.earliest_deadline_first,
            "RR": self.round_robin,
            "CRS": self.critical_ratio_scheduling,    # New: Critical Ratio Scheduling
            "HRRN": self.highest_response_ratio_next   # New: Highest Response Ratio Next
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
        self.task_counter = 0  # Unique counter for tasks

    def get_least_loaded_worker(self):
        return min(self.worker_loads.items(), key=lambda x: x[1])[0]

    def update_worker_load(self, worker_id, task_load):
        self.worker_loads[worker_id] = task_load

    def add_task(self, task):
        algorithm = task.get("scheduling", "RM")
        if algorithm == "RM":
            priority = task.get("execution_time", float('inf'))
        elif algorithm == "EDF":
            priority = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
        else:
            priority = 0

        counter = self.task_counter
        self.task_counter += 1

        heapq.heappush(self.tasks, (priority, counter, task))
        print(f"Added task {task.get('task_id')} to scheduler, assigned to {self.get_least_loaded_worker()}")
        return task

    def rate_monotonic(self):
        if not self.tasks:
            return None
        current_time = time.time()
        valid_tasks = []
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            if current_time >= task.get("arrival_time", 0):
                valid_tasks.append((priority, counter, task))
        if not valid_tasks:
            return None
        valid_tasks.sort(key=lambda x: x[2].get("execution_time", float('inf')))
        selected_task = valid_tasks[0]
        for i, task_tuple in enumerate(valid_tasks):
            if i > 0:
                heapq.heappush(self.tasks, task_tuple)
        return selected_task[2]

    def earliest_deadline_first(self):
        if not self.tasks:
            return None
        current_time = time.time()
        valid_tasks = []
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            if current_time >= task.get("arrival_time", 0):
                abs_deadline = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
                if current_time <= abs_deadline:
                    valid_tasks.append((priority, counter, task))
        if not valid_tasks:
            for priority, counter, task in valid_tasks:
                heapq.heappush(self.tasks, (priority, counter, task))
            return None
        selected_task = min(valid_tasks, key=lambda x: x[0])
        for priority, counter, task in valid_tasks:
            if (priority, counter, task) != selected_task:
                heapq.heappush(self.tasks, (priority, counter, task))
        return selected_task[2]

    def round_robin(self):
        if not self.tasks:
            return None
        current_time = time.time()
        valid_tasks = []
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            if current_time >= task.get("arrival_time", 0):
                valid_tasks.append((priority, counter, task))
        if not valid_tasks:
            return None
        valid_tasks.sort(key=lambda x: x[1])
        selected_task = valid_tasks[0]
        for i, task_tuple in enumerate(valid_tasks):
            if i > 0:
                heapq.heappush(self.tasks, task_tuple)
        return selected_task[2]

    def critical_ratio_scheduling(self):
        """Select task with lowest critical ratio = (absolute_deadline - current_time) / execution_time"""
        if not self.tasks:
            return None
        current_time = time.time()
        valid_tasks = []
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            if current_time >= task.get("arrival_time", 0):
                absolute_deadline = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
                critical_ratio = (absolute_deadline - current_time) / task.get("execution_time", 1)
                valid_tasks.append((critical_ratio, counter, task))
        if not valid_tasks:
            return None
        valid_tasks.sort(key=lambda x: x[0])
        selected_task = valid_tasks[0][2]
        for i, task_tuple in enumerate(valid_tasks):
            if i > 0:
                heapq.heappush(self.tasks, task_tuple)
        return selected_task

    def highest_response_ratio_next(self):
        """Select task with highest response ratio = (waiting_time + execution_time) / execution_time"""
        if not self.tasks:
            return None
        current_time = time.time()
        valid_tasks = []
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            if current_time >= task.get("arrival_time", 0):
                waiting_time = current_time - task.get("arrival_time", 0)
                response_ratio = (waiting_time + task.get("execution_time", 1)) / task.get("execution_time", 1)
                valid_tasks.append((response_ratio, counter, task))
        if not valid_tasks:
            return None
        valid_tasks.sort(key=lambda x: x[0], reverse=True)
        selected_task = valid_tasks[0][2]
        for i, task_tuple in enumerate(valid_tasks):
            if i > 0:
                heapq.heappush(self.tasks, task_tuple)
        return selected_task

    def schedule(self, algorithm):
        if algorithm not in self.scheduling_algorithms:
            raise ValueError(f"Unknown scheduling algorithm: {algorithm}")
        scheduled_task = self.scheduling_algorithms[algorithm]()
        if scheduled_task:
            print(f"\n[SCHEDULING DECISION] {algorithm}")
            print(f"  Selected task: {scheduled_task['task_id']}")
            if algorithm == "RM":
                print(f"  Execution time: {scheduled_task['execution_time']} (shorter execution = higher priority)")
            elif algorithm == "EDF":
                absolute_deadline = scheduled_task.get('arrival_time', 0) + scheduled_task.get('deadline', 0)
                print(f"  Absolute deadline: {absolute_deadline:.2f} (earlier deadline = higher priority)")
            elif algorithm == "RR":
                print("  Round Robin scheduling (equal priority)")
            elif algorithm == "CRS":
                absolute_deadline = scheduled_task.get('arrival_time', 0) + scheduled_task.get('deadline', 0)
                computed_ratio = (absolute_deadline - time.time()) / scheduled_task.get("execution_time", 1)
                print(f"  Critical Ratio: {computed_ratio:.2f} (lower ratio indicates higher urgency)")
            elif algorithm == "HRRN":
                waiting_time = time.time() - scheduled_task.get("arrival_time", 0)
                response_ratio = (waiting_time + scheduled_task.get("execution_time", 1)) / scheduled_task.get("execution_time", 1)
                print(f"  Response Ratio: {response_ratio:.2f} (higher ratio indicates higher priority)")
            print(f"  Assigned to worker: {scheduled_task['assigned_worker']}")
            self.schedule_history.append({
                "time": time.time(),
                "task": scheduled_task,
                "algorithm": algorithm
            })
        return scheduled_task

    def get_schedule_history(self):
        return self.schedule_history

class MQTTBroker:
    def __init__(self, host="localhost", port=1883, client_id="mqtt_broker"):
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
        self.worker_timeout = 5.0

        self.logger = logging.getLogger("MQTTBroker")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        self.client.max_inflight_messages_set(100)
        self.client.max_queued_messages_set(0)
        self.default_qos = 0

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_subscribe = self.on_subscribe
        self.client.on_publish = self.on_publish

    def on_connect(self, client, userdata, flags, rc):
        print(f"Broker connected with result code {rc}")
        client.subscribe("client/register", qos=0)
        client.subscribe("task/submit", qos=0)
        client.subscribe("task/status", qos=0)
        client.subscribe("system/control", qos=0)
        client.subscribe("worker/heartbeat", qos=0)

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
                    if self.worker_status[worker_id]["heartbeat_count"] % 5 == 0:
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
        client_id = data.get("client_id")
        if client_id in self.worker_status:
            self.worker_status[client_id]["active"] = True
            self.worker_status[client_id]["last_seen"] = time.time()
            self.worker_status[client_id]["current_load"] = data.get("current_load", 0.0)
            self.worker_status[client_id]["heartbeat_count"] = 0
            print(f"\n[WORKER REGISTERED] {client_id}")
            print(f"  Current load: {self.worker_status[client_id]['current_load']}")
            self.client.publish(
                f"client/{client_id}/registered",
                json.dumps({"status": "success", "timestamp": time.time()})
            )
            active_workers = [w_id for w_id, status in self.worker_status.items() if status["active"]]
            print(f"  Active workers: {active_workers}")

    def handle_task_submission(self, data):
        try:
            if isinstance(data, dict) and 'batch_submission' in data and 'tasks' in data:
                batch_data = data
                tasks_list = batch_data.get('tasks', [])
                scheduling = batch_data.get('scheduling', 'RM')
                timestamp = time.time()
                active_workers = {w_id: self.worker_status[w_id]["current_load"]
                                  for w_id in self.worker_status if self.worker_status[w_id].get("active", False)}
                if not active_workers:
                    active_workers = {w_id: 0.0 for w_id in self.worker_status.keys()}
                for task_data in tasks_list:
                    task_id = task_data.get('task_id')
                    execution_time = float(task_data.get('execution_time', 1.0))
                    task = {
                        'task_id': task_id,
                        'execution_time': execution_time,
                        'deadline': float(task_data.get('deadline', 10.0)),
                        'arrival_time': timestamp,
                        'scheduling': scheduling,
                        'status': 'pending'
                    }
                    for key in ['load_type', 'priority', 'burst_type', 'type']:
                        if key in task_data:
                            task[key] = task_data[key]
                    worker_id = min(active_workers.items(), key=lambda x: x[1])[0]
                    task['assigned_worker'] = worker_id
                    active_workers[worker_id] += execution_time
                    self.task_scheduler.add_task(task)
                self.logger.info(f"Processed batch of {len(tasks_list)} tasks with {scheduling} scheduling")
            elif isinstance(data, list):
                self.logger.info(f"Processing list batch of {len(data)} tasks")
                submission_time = time.time()
                for task_data in data:
                    task_id = task_data.get('task_id', f"task_{str(uuid.uuid4())[:8]}")
                    task = {
                        'task_id': task_id,
                        'execution_time': float(task_data.get('execution_time', 1.0)),
                        'deadline': float(task_data.get('deadline', 5.0)),
                        'arrival_time': submission_time,
                        'scheduling': task_data.get('scheduling_algorithm', 'RM'),
                        'status': 'pending',
                        'assigned_worker': None
                    }
                    if 'load_type' in task_data:
                        task['load_type'] = task_data['load_type']
                    if 'priority' in task_data:
                        task['priority'] = task_data['priority']
                    if 'burst_type' in task_data:
                        task['burst_type'] = task_data['burst_type']
                    if 'type' in task_data:
                        task['type'] = task_data['type']
                    worker_id = self.get_least_loaded_worker()
                    task['assigned_worker'] = worker_id
                    self.task_scheduler.add_task(task)
                    print(f"Added task {task_id} to scheduler, assigned to {worker_id}")
                    self.logger.info(f"Added task {task_id} to scheduler")
            else:
                task_data = data
                submission_time = time.time()
                task_id = task_data.get('task_id', f"task_{str(uuid.uuid4())[:8]}")
                task = {
                    'task_id': task_id,
                    'execution_time': float(task_data.get('execution_time', 1.0)),
                    'deadline': float(task_data.get('deadline', 5.0)),
                    'arrival_time': submission_time,
                    'scheduling': task_data.get('scheduling_algorithm', 'RM'),
                    'status': 'pending',
                    'assigned_worker': None
                }
                if 'load_type' in task_data:
                    task['load_type'] = task_data['load_type']
                if 'priority' in task_data:
                    task['priority'] = task_data['priority']
                if 'burst_type' in task_data:
                    task['burst_type'] = task_data['burst_type']
                if 'type' in task_data:
                    task['type'] = task_data['type']
                worker_id = self.get_least_loaded_worker()
                task['assigned_worker'] = worker_id
                self.task_scheduler.add_task(task)
                print(f"Added task {task_id} to scheduler, assigned to {worker_id}")
                self.logger.info(f"Added task {task_id} to scheduler")
        except Exception as e:
            self.logger.error(f"Error handling task submission: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def handle_task_status(self, data):
        task_id = data.get("task_id")
        worker_id = data.get("worker_id")
        status = data.get("status")
        execution_time = data.get("actual_execution_time", 0)
        if task_id and worker_id and status:
            if status == "duplicate":
                return
            print(f"\n[TASK COMPLETED] Task {task_id}")
            print(f"  Worker: {worker_id}")
            print(f"  Status: {status}")
            print(f"  Execution Time: {execution_time:.2f}s")
            if status != "completed":
                print(f"  Note: Deadline missed!")
            self.task_status[task_id] = {
                "worker_id": worker_id,
                "status": status,
                "execution_time": execution_time,
                "timestamp": time.time()
            }
            if worker_id in self.worker_status:
                self.worker_status[worker_id]["last_seen"] = time.time()
                self.worker_status[worker_id]["current_load"] = data.get("current_load", 0.0)
                self.task_scheduler.update_worker_load(worker_id, data.get("current_load", 0.0))

    def handle_system_control(self, data):
        command = data.get("command")
        if command == "get_schedule_history":
            history = self.task_scheduler.get_schedule_history()
            self.client.publish("system/schedule_history", json.dumps(history))
        elif command == "get_task_status":
            task_id = data.get("task_id")
            if task_id in self.task_status:
                self.client.publish(f"task/{task_id}/status", json.dumps(self.task_status[task_id]))
            else:
                self.client.publish(f"task/{task_id}/status", json.dumps({"status": "unknown", "timestamp": time.time()}))

    def on_subscribe(self, client, userdata, mid, granted_qos):
        print(f"Subscribed to topic with QoS: {granted_qos}")

    def on_publish(self, client, userdata, mid):
        print(f"Message {mid} published")

    def start(self):
        self.client.connect(self.host, self.port, 60)
        self.client.subscribe("client/register")
        self.client.subscribe("task/status")
        self.client.subscribe("worker/heartbeat")
        self.client.subscribe("task/submit")
        self.client.loop_start()
        self.scheduler_thread = threading.Thread(target=self.run_scheduler)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        self.logger.info(f"MQTT Broker started on {self.host}:{self.port}")

    def run_scheduler(self):
        while True:
            try:
                current_time = time.time()
                if not hasattr(self, 'last_health_check') or current_time - self.last_health_check > 2.0:
                    for worker_id, status in self.worker_status.items():
                        if status["active"]:
                            time_since_last_seen = current_time - status["last_seen"]
                            if time_since_last_seen > self.worker_timeout:
                                status["active"] = False
                                print(f"\n[WORKER STATUS] Worker {worker_id} marked as inactive")
                                print(f"  Last seen: {time_since_last_seen:.1f} seconds ago")
                    self.last_health_check = current_time
                active_workers = [w_id for w_id, status in self.worker_status.items() if status["active"]]
                if self.task_scheduler.tasks and active_workers:
                    _, _, current_task = self.task_scheduler.tasks[0]
                    algorithm = current_task.get("scheduling", "RM")
                    if algorithm in self.task_scheduler.scheduling_algorithms:
                        task = self.task_scheduler.schedule(algorithm)
                        if task:
                            worker_id = task["assigned_worker"]
                            if self.worker_status[worker_id]["active"]:
                                self.client.publish(
                                    f"worker/{worker_id}/task",
                                    json.dumps({
                                        "task_id": task["task_id"],
                                        "execution_time": task["execution_time"],
                                        "deadline": task["deadline"],
                                        "algorithm": task["scheduling"],
                                        "load_type": task.get("load_type", "unknown"),
                                        "priority": task.get("priority", "unknown"),
                                        "burst_type": task.get("burst_type", "unknown"),
                                        "timestamp": time.time()
                                    })
                                )
                                print(f"\n[TASK ASSIGNED] Task {task['task_id']} to {worker_id}")
                            else:
                                alternative_worker = self.get_least_loaded_worker()
                                task["assigned_worker"] = alternative_worker
                                self.client.publish(
                                    f"worker/{alternative_worker}/task",
                                    json.dumps({
                                        "task_id": task["task_id"],
                                        "execution_time": task["execution_time"],
                                        "deadline": task["deadline"],
                                        "algorithm": task["scheduling"],
                                        "load_type": task.get("load_type", "unknown"),
                                        "priority": task.get("priority", "unknown"),
                                        "burst_type": task.get("burst_type", "unknown"),
                                        "timestamp": time.time()
                                    })
                                )
                                print(f"\n[TASK REASSIGNED] Task {task['task_id']} to {alternative_worker}")
                time.sleep(0.01)
            except Exception as e:
                print(f"Error in scheduler: {e}")

    def get_system_status(self):
        return {
            "workers": self.worker_status,
            "tasks_pending": len(self.task_scheduler.tasks),
            "tasks_completed": len([t for t in self.task_status.values() if t["status"] == "completed"]),
            "load_distribution": {
                worker_id: status["current_load"]
                for worker_id, status in self.worker_status.items() if status["active"]
            }
        }

    def get_least_loaded_worker(self):
        active_workers = {w_id: status for w_id, status in self.worker_status.items() if status.get("active", False)}
        if not active_workers:
            return next(iter(self.worker_status.keys()))
        return min(active_workers.items(), key=lambda x: x[1].get("current_load", 0))[0]

if __name__ == "__main__":
    broker = MQTTBroker()
    broker.start()
    try:
        print("Broker running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down broker...")
