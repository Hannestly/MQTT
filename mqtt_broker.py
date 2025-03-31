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
        """Add a new task to the scheduler - optimized for EDF"""
        algorithm = task.get("scheduling", "RM")
        
        # Get a unique counter
        counter = self.task_counter
        self.task_counter += 1
        
        # Set priority based on algorithm
        if algorithm == "RM":
            # For RM, priority is based on execution time
            priority = task.get("execution_time", float('inf'))
        elif algorithm == "EDF":
            # For EDF, priority is absolute deadline
            priority = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
            # Store the absolute deadline directly in the task for faster access
            task["abs_deadline"] = priority
        else:
            # For RR, all tasks have the same priority
            priority = 0
        
        # Add to priority queue
        heapq.heappush(self.tasks, (priority, counter, task))
        
        # Assign worker based on load
        worker_id = self.get_least_loaded_worker()
        task["assigned_worker"] = worker_id
        
        # Only do minimal logging
        if algorithm == "EDF":
            print(f"Added EDF task {task.get('task_id')} (deadline: {task.get('deadline')})")
        else:
            print(f"Added task {task.get('task_id')} to {worker_id}")
        
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
        """Simplified and optimized EDF implementation"""
        if not self.tasks:
            return None

        # Track the best task
        best_task = None
        best_deadline = float('inf')
        best_priority = float('inf')
        best_counter = 0
        
        # Store tasks that we'll keep
        keep_tasks = []
        current_time = time.time()
        
        # Extract all tasks once
        while self.tasks:
            priority, counter, task = heapq.heappop(self.tasks)
            
            # Skip tasks that aren't ready yet
            if current_time < task.get("arrival_time", 0):
                keep_tasks.append((priority, counter, task))
                continue
            
            # Calculate absolute deadline
            abs_deadline = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
            
            # Check if this task has earlier deadline
            if abs_deadline < best_deadline:
                # If we already had a best task, add it back to the keep list
                if best_task:
                    keep_tasks.append((best_priority, best_counter, best_task))
                
                # Update our tracking of the best task
                best_task = task
                best_deadline = abs_deadline
                best_priority = priority
                best_counter = counter
            else:
                # Not the best, so keep it
                keep_tasks.append((priority, counter, task))
        
        # Put all tasks we're keeping back into the heap
        for task_item in keep_tasks:
            heapq.heappush(self.tasks, task_item)
        
        # Only do debug logging if we actually found a task
        if best_task:
            print(f"[EDF] Selected task {best_task['task_id']} with deadline {best_deadline:.2f}")
        
        return best_task

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
            
            # At the end, after processing all tasks, force immediate distribution:
            if isinstance(data, dict) and 'batch_submission' in data:
                # Get active workers
                active_workers = [w_id for w_id, status in self.worker_status.items() 
                                 if status["active"]]
                
                # Distribute initial tasks immediately (one to each worker)
                worker_index = 0
                while self.task_scheduler.tasks and worker_index < len(active_workers):
                    # Take the next task
                    _, _, task = heapq.heappop(self.task_scheduler.tasks)
                    worker_id = active_workers[worker_index]
                    
                    # Send directly
                    self.client.publish(
                        f"worker/{worker_id}/task",
                        json.dumps({
                            "task_id": task["task_id"],
                            "execution_time": task["execution_time"],
                            "deadline": task["deadline"],
                            "algorithm": task.get("scheduling", "RM"),
                            "load_type": task.get("load_type", "unknown"),
                            "timestamp": time.time()
                        }),
                        qos=0
                    )
                    
                    print(f"[IMMEDIATE DISTRIBUTE] Task {task['task_id']} to {worker_id}")
                    worker_index += 1
            
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

            # At the end, print active workers and their loads
            print("\n[WORKER STATUS AFTER COMPLETION]")
            for worker_id, status in self.worker_status.items():
                if status["active"]:
                    print(f"  {worker_id}: Load = {status['current_load']:.2f}, Tasks completed = {sum(1 for t in self.task_status.values() if t.get('worker_id') == worker_id)}")

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
        
        # Start the dedicated EDF scheduler
        self.edf_thread = threading.Thread(target=self.direct_edf_scheduling)
        self.edf_thread.daemon = True
        self.edf_thread.start()
        
        self.logger.info(f"MQTT Broker started on {self.host}:{self.port}")

    def run_scheduler(self):
        """Ensure continuous task assignment without waiting for completion"""
        while True:
            try:
                # Process as many tasks as possible in each cycle
                max_assignments_per_cycle = 5  # Assign up to 5 tasks per cycle
                
                current_time = time.time()
                active_workers = [w_id for w_id, status in self.worker_status.items() 
                                 if status["active"]]
                
                assignments_this_cycle = 0
                
                # Assign multiple tasks in a single cycle
                while self.task_scheduler.tasks and active_workers and assignments_this_cycle < max_assignments_per_cycle:
                    # Get scheduling algorithm from the first task
                    _, _, current_task = self.task_scheduler.tasks[0]
                    algorithm = current_task.get("scheduling", "RM")
                    if algorithm in self.task_scheduler.scheduling_algorithms:
                        task = self.task_scheduler.schedule(algorithm)
                        if task:
                            # Get best worker for this task
                            worker_id = self.get_least_loaded_worker()
                            task["assigned_worker"] = worker_id
                            
                            # Publish task to worker
                            self.client.publish(
                                f"worker/{worker_id}/task",
                                json.dumps({
                                    "task_id": task["task_id"],
                                    "execution_time": task["execution_time"],
                                    "deadline": task["deadline"],
                                    "algorithm": task["scheduling"],
                                    "load_type": task.get("load_type", "unknown"),
                                    "timestamp": time.time()
                                }),
                                qos=0
                            )
                            
                            print(f"[PARALLEL ASSIGN] Task {task['task_id']} to {worker_id}")
                            assignments_this_cycle += 1
                        else:
                            break  # No task to schedule
                    else:
                        break  # Unknown algorithm
                
                # Short sleep between cycles
                time.sleep(0.01)
                
            except Exception as e:
                print(f"Error in scheduler: {e}")
                time.sleep(0.1)

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
        """Improved worker assignment to ensure parallelism"""
        active_workers = {w_id: status["current_load"] 
                         for w_id, status in self.worker_status.items() 
                         if status.get("active", False)}
        
        if not active_workers:
            return next(iter(self.worker_status.keys()))
        
        # Get worker assignment counts for recently assigned tasks
        recent_assignments = {}
        for priority, _, task in self.task_scheduler.tasks[:10]:  # Look at last 10 tasks
            worker = task.get("assigned_worker")
            if worker:
                recent_assignments[worker] = recent_assignments.get(worker, 0) + 1
        
        # Prioritize workers with fewer recent assignments
        worker_scores = {}
        for worker_id, load in active_workers.items():
            # Score based on load and recent assignments
            assignment_count = recent_assignments.get(worker_id, 0)
            worker_scores[worker_id] = load + (assignment_count * 0.5)  # Penalize recent assignments
        
        # Return worker with lowest score
        result = min(worker_scores.items(), key=lambda x: x[1])[0]
        print(f"[ASSIGNMENT] Selected {result} with score {worker_scores[result]:.2f}")
        return result

    def direct_edf_scheduling(self):
        """Completely standalone EDF implementation that bypasses regular scheduling"""
        while True:
            try:
                # Only process if we have tasks and workers
                if self.task_scheduler.tasks:
                    current_time = time.time()
                    active_workers = [w_id for w_id, status in self.worker_status.items() 
                                    if status["active"]]
                    
                    if not active_workers:
                        time.sleep(0.01)
                        continue
                    
                    # Get all EDF tasks
                    edf_tasks = []
                    other_tasks = []
                    
                    while self.task_scheduler.tasks:
                        priority, counter, task = heapq.heappop(self.task_scheduler.tasks)
                        if task.get("scheduling") == "EDF":
                            if current_time >= task.get("arrival_time", 0):
                                edf_tasks.append(task)
                            else:
                                other_tasks.append((priority, counter, task))
                        else:
                            other_tasks.append((priority, counter, task))
                    
                    # Put back non-EDF tasks
                    for task_tuple in other_tasks:
                        heapq.heappush(self.task_scheduler.tasks, task_tuple)
                    
                    if edf_tasks:
                        # Sort by absolute deadline
                        edf_tasks.sort(key=lambda t: t.get("arrival_time", 0) + t.get("deadline", float('inf')))
                        
                        # Take the task with earliest absolute deadline
                        chosen_task = edf_tasks[0]
                        
                        # Find best worker (fastest)
                        best_worker = min([(w_id, self.worker_status[w_id]["current_load"]) 
                                         for w_id in active_workers], key=lambda x: x[1])[0]
                        
                        # Send directly
                        print(f"[DIRECT EDF] Sending task {chosen_task['task_id']} to {best_worker}")
                        self.client.publish(
                            f"worker/{best_worker}/task",
                            json.dumps({
                                "task_id": chosen_task["task_id"],
                                "execution_time": chosen_task["execution_time"],
                                "deadline": chosen_task["deadline"],
                                "algorithm": "EDF",
                                "timestamp": time.time()
                            }),
                            qos=0
                        )
                        
                        # Put back other EDF tasks
                        for task in edf_tasks[1:]:
                            priority = task.get("arrival_time", 0) + task.get("deadline", float('inf'))
                            counter = self.task_scheduler.task_counter
                            self.task_scheduler.task_counter += 1
                            heapq.heappush(self.task_scheduler.tasks, (priority, counter, task))
                
                time.sleep(0.01)
            except Exception as e:
                print(f"Direct EDF error: {e}")
                time.sleep(0.1)

if __name__ == "__main__":
    broker = MQTTBroker()
    broker.start()
    try:
        print("Broker running. Press Ctrl+C to exit.")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down broker...")
