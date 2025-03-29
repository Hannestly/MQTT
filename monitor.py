# monitor.py
import paho.mqtt.client as mqtt
import time
import json
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np
import logging


class SystemMonitor:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="monitor")
        self.task_history = []
        self.schedule_history = []
        self.worker_status = defaultdict(dict)
        self.worker_history = defaultdict(list)
        self.batch_submissions = []  # Track batch submissions
        self.task_sets = defaultdict(list)  # Group tasks by their set

        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

        # Initialize logger
        self.logger = logging.getLogger("SystemMonitor")
        self.logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

    def on_connect(self, client, userdata, flags, rc):
        print(f"Monitor connected with result code {rc}")
        # Subscribe to relevant topics
        client.subscribe("task/status")
        client.subscribe("system/schedule_history")
        client.subscribe("worker/+/task_history")
        client.subscribe("worker/heartbeat")
        client.subscribe("task/submit")  # Add subscription to track task submissions

    def on_message(self, client, userdata, message):
        try:
            topic = message.topic
            payload = message.payload.decode()

            data = json.loads(payload)
            # Only print non-heartbeat messages
            if topic != "worker/heartbeat":
                print(f"\nMonitor received message on {topic}:")
                if topic != "task/submit":  # Don't print full task submission data
                    print(f"  Data: {data}")
                else:
                    print(f"  Received task submission with {len(data.get('tasks', []))} tasks")

            # Process message based on topic
            if topic == "task/status":
                self.task_history.append(data)
                    
                # Try to identify which task set this belongs to
                task_id = data.get("task_id")
                for set_name, tasks in self.task_sets.items():
                    if task_id in tasks:
                        data["task_set_name"] = set_name
                        break
            
            elif topic == "system/schedule_history":
                if isinstance(data, list):
                    self.schedule_history.extend(data)
            
                elif topic == "worker/heartbeat":
                    self.update_worker_status(data)
            
                elif topic.startswith("worker/") and topic.endswith("/task_history"):
                    worker_id = topic.split("/")[1]
                if isinstance(data, list):
                        self.worker_history[worker_id].extend(data)
            
                elif topic == "task/submit":
                    self.logger.info(f"Received task batch: {data}")
                    # Track batch submissions
                    if data.get("batch_submission", False):
                        batch_info = {
                            "timestamp": data.get("timestamp", time.time()),
                            "scheduling": data.get("scheduling"),
                            "task_count": len(data.get("tasks", [])),
                            "task_ids": [t.get("task_id") for t in data.get("tasks", [])],
                            "task_set_type": data.get("tasks", [])[0].get("type") if data.get("tasks") else None
                        }
                        
                        # Store task IDs by set type for later reference
                        set_type = batch_info.get("task_set_type") or "unknown"
                        scheduling = batch_info.get("scheduling")
                        set_name = f"{set_type}_{scheduling}"
                        self.task_sets[set_name] = batch_info["task_ids"]
                        
                        self.batch_submissions.append(batch_info)
                        print(f"  Recorded batch submission: {set_name} with {batch_info['task_count']} tasks")
        
        except Exception as e:
            self.logger.error(f"Error processing message on {topic}: {str(e)}")
            import traceback
            self.logger.error(traceback.format_exc())

    def update_worker_status(self, data):
        """Update worker status from heartbeat"""
        worker_id = data.get("worker_id")
        if worker_id:
            self.worker_status[worker_id].update({
                "last_seen": data.get("timestamp"),
                "current_load": data.get("current_load"),
                "queue_size": data.get("queue_size"),
                "is_busy": data.get("is_busy")
            })

    def start(self):
        """Start the monitor"""
        self.mqtt_client.connect(self.broker_host, self.broker_port)
        print("System Monitor started")

        # Request initial data
        self.mqtt_client.publish(
            "system/control", json.dumps({"command": "get_schedule_history"})
        )

        self.mqtt_client.loop_start()

    def stop(self):
        """Stop the monitor"""
        self.mqtt_client.loop_stop()

    def plot_worker_loads(self):
        """Plot worker load distribution"""
        if not self.worker_status:
            print("No worker status to plot")
            return

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))

        # Current load distribution
        worker_ids = list(self.worker_status.keys())
        loads = [status["current_load"] for status in self.worker_status.values()]
        queue_sizes = [status["queue_size"] for status in self.worker_status.values()]

        x = np.arange(len(worker_ids))
        width = 0.35

        ax1.bar(x - width/2, loads, width, label='Current Load')
        ax1.bar(x + width/2, queue_sizes, width, label='Queue Size')
        ax1.set_xlabel("Workers")
        ax1.set_ylabel("Tasks")
        ax1.set_title("Worker Load Distribution")
        ax1.set_xticks(x)
        ax1.set_xticklabels(worker_ids)
        ax1.legend()

        # Task type distribution per worker
        task_types = defaultdict(lambda: defaultdict(int))
        for task in self.task_history:
            worker_id = task.get("worker_id")
            if worker_id:
                if "load_type" in task:
                    task_types[worker_id]["load_" + task["load_type"]] += 1
                if "priority" in task:
                    task_types[worker_id]["priority_" + task["priority"]] += 1
                if "burst_type" in task:
                    task_types[worker_id]["burst_" + task["burst_type"]] += 1

        # Plot task type distribution
        bottoms = np.zeros(len(worker_ids))
        for task_type in set().union(*[d.keys() for d in task_types.values()]):
            values = [task_types[w][task_type] for w in worker_ids]
            ax2.bar(worker_ids, values, bottom=bottoms, label=task_type)
            bottoms += values

        ax2.set_xlabel("Workers")
        ax2.set_ylabel("Number of Tasks")
        ax2.set_title("Task Type Distribution per Worker")
        ax2.legend()

        plt.tight_layout()
        plt.show()

    def plot_task_completion(self):
        """Plot task completion statistics with algorithm and task set type analysis"""
        if not self.task_history:
            print("No task history to plot")
            return

        fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 12))

        # Task completion by algorithm and task set type
        alg_set_stats = defaultdict(lambda: defaultdict(lambda: {"completed": 0, "missed": 0}))
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            set_type = task.get("task_set_type", "unknown")
            status = "completed" if task["status"] == "completed" else "missed"
            alg_set_stats[alg][set_type][status] += 1

        # Plot completion rates by algorithm
        algorithms = list(alg_set_stats.keys())
        x = np.arange(len(algorithms))
        width = 0.25
        set_types = ["mixed_utilization", "varying_priority", "burst"]
        colors = ["skyblue", "lightgreen", "salmon"]

        for i, set_type in enumerate(set_types):
            completed_rates = []
            for alg in algorithms:
                total = (alg_set_stats[alg][set_type]["completed"] + 
                        alg_set_stats[alg][set_type]["missed"])
                rate = (alg_set_stats[alg][set_type]["completed"] / total * 100 
                       if total > 0 else 0)
                completed_rates.append(rate)
            
            ax1.bar(x + (i-1)*width, completed_rates, width, 
                   label=set_type, color=colors[i])

        ax1.set_ylabel("Completion Rate (%)")
        ax1.set_title("Task Completion Rate by Algorithm and Set Type")
        ax1.set_xticks(x)
        ax1.set_xticklabels(algorithms)
        ax1.legend()

        # Response times by algorithm and set type
        response_times = defaultdict(lambda: defaultdict(list))
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            set_type = task.get("task_set_type", "unknown")
            response_times[alg][set_type].append(task["response_time"])

        # Plot average response times
        for i, set_type in enumerate(set_types):
            avg_responses = []
            std_responses = []
            for alg in algorithms:
                times = response_times[alg][set_type]
                avg_responses.append(np.mean(times) if times else 0)
                std_responses.append(np.std(times) if times else 0)
            
            ax2.bar(x + (i-1)*width, avg_responses, width, 
                   yerr=std_responses, capsize=5,
                   label=set_type, color=colors[i])

        ax2.set_ylabel("Response Time (s)")
        ax2.set_title("Average Response Time by Algorithm and Set Type")
        ax2.set_xticks(x)
        ax2.set_xticklabels(algorithms)
        ax2.legend()

        # Worker load distribution
        worker_loads = defaultdict(lambda: defaultdict(list))
        for task in self.task_history:
            worker_id = task.get("worker_id", "unknown")
            set_type = task.get("task_set_type", "unknown")
            worker_loads[worker_id][set_type].append(task.get("current_load", 0))

        workers = list(worker_loads.keys())
        x = np.arange(len(workers))

        for i, set_type in enumerate(set_types):
            avg_loads = []
            std_loads = []
            for worker in workers:
                loads = worker_loads[worker][set_type]
                avg_loads.append(np.mean(loads) if loads else 0)
                std_loads.append(np.std(loads) if loads else 0)
            
            ax3.bar(x + (i-1)*width, avg_loads, width, 
                   yerr=std_loads, capsize=5,
                   label=set_type, color=colors[i])

        ax3.set_ylabel("Average Load")
        ax3.set_title("Worker Load by Task Set Type")
        ax3.set_xticks(x)
        ax3.set_xticklabels(workers)
        ax3.legend()

        # Deadline miss rates
        deadline_stats = defaultdict(lambda: defaultdict(lambda: {"total": 0, "missed": 0}))
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            set_type = task.get("task_set_type", "unknown")
            deadline_stats[alg][set_type]["total"] += 1
            if task["status"] != "completed":
                deadline_stats[alg][set_type]["missed"] += 1

        for i, set_type in enumerate(set_types):
            miss_rates = []
            for alg in algorithms:
                stats = deadline_stats[alg][set_type]
                miss_rates.append(stats["missed"] / stats["total"] * 100 
                                if stats["total"] > 0 else 0)
            
            ax4.bar(x + (i-1)*width, miss_rates, width, 
                   label=set_type, color=colors[i])

        ax4.set_ylabel("Deadline Miss Rate (%)")
        ax4.set_title("Deadline Miss Rate by Algorithm and Set Type")
        ax4.set_xticks(x)
        ax4.set_xticklabels(algorithms)
        ax4.legend()

        plt.tight_layout()
        plt.show()

    def plot_response_times(self):
        """Plot task response times and deadline analysis"""
        if not self.task_history:
            print("No task history to plot")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Response times
        algorithms = defaultdict(list)
        deadline_margins = defaultdict(list)
        
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            algorithms[alg].append(task["response_time"])

            # Calculate deadline margin (deadline - actual_execution_time)
            actual_time = task.get("actual_execution_time", 0)
            deadline = task.get("deadline", 0)
            deadline_margins[alg].append(deadline - actual_time)

        # Plot average response times
        algs = list(algorithms.keys())
        avg_response = [np.mean(times) for times in algorithms.values()]
        std_response = [np.std(times) for times in algorithms.values()]

        ax1.bar(algs, avg_response, yerr=std_response, capsize=5)
        ax1.set_xlabel("Scheduling Algorithm")
        ax1.set_ylabel("Response Time (s)")
        ax1.set_title("Average Response Time with Standard Deviation")

        # Plot deadline margins
        avg_margins = [np.mean(margins) for margins in deadline_margins.values()]
        std_margins = [np.std(margins) for margins in deadline_margins.values()]

        ax2.bar(algs, avg_margins, yerr=std_margins, capsize=5, color='lightgreen')
        ax2.axhline(y=0, color='r', linestyle='--', label='Deadline')
        ax2.set_xlabel("Scheduling Algorithm")
        ax2.set_ylabel("Margin (s)")
        ax2.set_title("Average Deadline Margin")
        
        plt.tight_layout()
        plt.show()

    def plot_schedule_timeline(self):
        """Plot enhanced timeline of scheduled tasks"""
        if not self.task_history:
            print("No task history to plot")
            return

        plt.figure(figsize=(15, 8))
        colors = {"RM": "blue", "EDF": "green", "RR": "red"}

        # Group tasks by algorithm
        alg_tasks = defaultdict(list)
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            start_time = task.get("start_time", 0)
            actual_execution_time = task.get("actual_execution_time", 0)
            task_id = task.get("task_id", "unknown")
            deadline_met = task.get("status") == "completed"
            
            alg_tasks[alg].append({
                "task_id": task_id,
                "start": start_time,
                "duration": actual_execution_time,
                "deadline_met": deadline_met
            })

        # Plot tasks as bars
        y_position = 0
        y_ticks = []
        y_labels = []
        
        for alg in alg_tasks:
            tasks = alg_tasks[alg]
            y_ticks.append(y_position + 0.5)
            y_labels.append(alg)
            
            for task in tasks:
                color = colors.get(alg, "gray")
                alpha = 1.0 if task["deadline_met"] else 0.5
                plt.barh(y_position, task["duration"], left=task["start"], 
                        color=color, alpha=alpha)
                plt.text(task["start"], y_position, task["task_id"], 
                        verticalalignment='bottom')
            
            y_position += 1

        plt.yticks(y_ticks, y_labels)
        plt.xlabel("Time (s)")
        plt.ylabel("Scheduling Algorithm")
        plt.title("Task Execution Timeline")
        plt.grid(True, axis='x')
        plt.tight_layout()
        plt.show()

    def plot_algorithm_performance(self):
        """Plot detailed algorithm performance analysis"""
        if not self.task_history:
            print("No task history to plot")
            return

        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 6))

        # Algorithm performance by task set type
        alg_perf = defaultdict(lambda: defaultdict(lambda: {
            "response_times": [],
            "completion_rate": 0,
            "total_tasks": 0
        }))

        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            set_type = task.get("task_set_type", "unknown")
            alg_perf[alg][set_type]["response_times"].append(task["response_time"])
            alg_perf[alg][set_type]["total_tasks"] += 1
            if task["status"] == "completed":
                alg_perf[alg][set_type]["completion_rate"] += 1

        # Plot response time comparison
        algorithms = list(alg_perf.keys())
        set_types = ["mixed_utilization", "varying_priority", "burst"]
        x = np.arange(len(algorithms))
        width = 0.25

        for i, set_type in enumerate(set_types):
            avg_response = []
            for alg in algorithms:
                times = alg_perf[alg][set_type]["response_times"]
                avg_response.append(np.mean(times) if times else 0)
            ax1.bar(x + (i-1)*width, avg_response, width, label=set_type)

        ax1.set_ylabel("Average Response Time (s)")
        ax1.set_title("Response Time by Algorithm and Task Set")
        ax1.set_xticks(x)
        ax1.set_xticklabels(algorithms)
        ax1.legend()

        # Plot completion rate comparison
        for i, set_type in enumerate(set_types):
            completion_rates = []
            for alg in algorithms:
                stats = alg_perf[alg][set_type]
                rate = (stats["completion_rate"] / stats["total_tasks"] * 100 
                       if stats["total_tasks"] > 0 else 0)
                completion_rates.append(rate)
            ax2.bar(x + (i-1)*width, completion_rates, width, label=set_type)

        ax2.set_ylabel("Completion Rate (%)")
        ax2.set_title("Completion Rate by Algorithm and Task Set")
        ax2.set_xticks(x)
        ax2.set_xticklabels(algorithms)
        ax2.legend()

        plt.tight_layout()
        plt.show()

    def plot_scheduling_timeline(self):
        """Plot detailed timeline of task scheduling and execution by algorithm"""
        if not self.task_history:
            print("No task history to plot")
            return

        # Group tasks by algorithm and worker
        task_data = defaultdict(lambda: defaultdict(list))
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            worker = task.get("worker_id", "unknown")
            
            # Calculate task start and end times
            start_time = task.get("start_time", 0)
            # If start_time is not available, estimate from completion time and execution time
            if start_time == 0 and "timestamp" in task and "actual_execution_time" in task:
                start_time = task["timestamp"] - task["actual_execution_time"]
                
            duration = task.get("actual_execution_time", 0)
            
            # Normalize time if we have batch submissions
            if self.batch_submissions:
                first_batch = min(sub["timestamp"] for sub in self.batch_submissions)
                start_time = start_time - first_batch
            
            task_data[alg][worker].append({
                "task_id": task.get("task_id", "unknown"),
                "start": start_time,
                "duration": duration,
                "deadline_met": task.get("status") == "completed",
                "load_type": task.get("load_type", "unknown"),
                "priority": task.get("priority", "unknown"),
                "burst_type": task.get("burst_type", "unknown")
            })

        # Plot tasks as bars on a timeline
        plt.figure(figsize=(15, 10))
        algorithms = list(task_data.keys())
        colors = {"RM": "blue", "EDF": "green", "RR": "red"}
        
        # Calculate number of rows needed (algorithms × workers)
        row_count = sum(len(workers) for workers in task_data.values())
        
        # Setup plot
        y_position = 0
        y_ticks = []
        y_labels = []
        
        # Plot tasks by algorithm and worker
        for alg in algorithms:
            for worker, tasks in task_data[alg].items():
                y_ticks.append(y_position + 0.5)
                y_labels.append(f"{alg} - {worker}")
                
                for task in tasks:
                    # Determine color based on task type and algorithm
                    base_color = colors.get(alg, "gray")
                    
                    # Adjust alpha for deadline status
                    alpha = 1.0 if task["deadline_met"] else 0.5
                    
                    # Plot the task execution bar
                    bar = plt.barh(y_position, task["duration"], left=task["start"], 
                                  color=base_color, alpha=alpha)
                    
                    # Add task details as text
                    details = task["task_id"]
                    if task.get("load_type") != "unknown":
                        details += f"\n{task['load_type']}"
                    elif task.get("priority") != "unknown":
                        details += f"\n{task['priority']}"
                    elif task.get("burst_type") != "unknown":
                        details += f"\n{task['burst_type']}"
                        
                    plt.text(task["start"], y_position, details, 
                            verticalalignment='center', fontsize=8)
                
                y_position += 1

        # Add submission points
        for batch in self.batch_submissions:
            # Normalize time
            if self.batch_submissions:
                first_batch = min(sub["timestamp"] for sub in self.batch_submissions)
                submission_time = batch["timestamp"] - first_batch
            else:
                submission_time = 0
                
            plt.axvline(x=submission_time, color='black', linestyle='--', 
                      label=f"Submission: {batch['task_set_type']} - {batch['scheduling']}")
            plt.text(submission_time, row_count+0.5, 
                    f"Submit: {batch.get('task_count')} tasks", 
                    rotation=90, verticalalignment='top')

        plt.yticks(y_ticks, y_labels)
        plt.xlabel("Time (seconds from first submission)")
        plt.ylabel("Algorithm - Worker")
        plt.title("Task Scheduling and Execution Timeline")
        plt.grid(True, axis='x')
        
        # Add legend for deadline status
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(color='gray', alpha=1.0, label='Met Deadline'),
            Patch(color='gray', alpha=0.5, label='Missed Deadline')
        ]
        for alg, color in colors.items():
            legend_elements.append(Patch(color=color, label=alg))
            
        plt.legend(handles=legend_elements, loc='upper right')
        
        plt.tight_layout()
        plt.show()

    def plot_algorithm_comparison(self):
        """Plot comparative analysis of algorithms across metrics"""
        if not self.task_history:
            print("No task history to plot")
            return

        # Group tasks by algorithm and task type
        alg_task_data = defaultdict(lambda: defaultdict(list))
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            
            # Determine task type
            if "load_type" in task:
                task_type = f"load_{task['load_type']}"
            elif "priority" in task:
                task_type = f"priority_{task['priority']}"
            elif "burst_type" in task:
                task_type = f"burst_{task['burst_type']}"
            else:
                task_type = "unknown"
                
            alg_task_data[alg][task_type].append(task)

        # Setup metrics to compare
        metrics = {
            "Response Time": lambda t: t.get("response_time", 0),
            "Execution Time": lambda t: t.get("actual_execution_time", 0),
            "Deadline Margin": lambda t: t.get("deadline", 0) - t.get("actual_execution_time", 0),
            "Completion Rate": lambda t: 100 if t.get("status") == "completed" else 0
        }

        # Create plot
        fig, axes = plt.subplots(len(metrics), 1, figsize=(12, 4*len(metrics)))
        
        for i, (metric_name, metric_func) in enumerate(metrics.items()):
            ax = axes[i]
            
            # Calculate metric values for each algorithm and task type
            algs = list(alg_task_data.keys())
            task_types = set()
            for task_dict in alg_task_data.values():
                task_types.update(task_dict.keys())
            task_types = sorted(list(task_types))
            
            x = np.arange(len(algs))
            width = 0.8 / len(task_types)
            
            for j, task_type in enumerate(task_types):
                values = []
                errors = []
                
                for alg in algs:
                    tasks = alg_task_data[alg][task_type]
                    if tasks:
                        if metric_name == "Completion Rate":
                            # Special case for completion rate
                            value = sum(metric_func(t) for t in tasks) / len(tasks)
                            error = 0
                        else:
                            # For other metrics, calculate mean and std
                            metric_values = [metric_func(t) for t in tasks]
                            value = np.mean(metric_values)
                            error = np.std(metric_values)
                        values.append(value)
                        errors.append(error)
                    else:
                        values.append(0)
                        errors.append(0)
                
                # Position the bars for this task type
                positions = x + (j - len(task_types)/2 + 0.5) * width
                
                # Plot with error bars if not completion rate
                if metric_name == "Completion Rate":
                    ax.bar(positions, values, width, label=task_type)
                else:
                    ax.bar(positions, values, width, yerr=errors, capsize=5, label=task_type)
            
            # Set up axis labels and title
            ax.set_xticks(x)
            ax.set_xticklabels(algs)
            ax.set_ylabel(metric_name)
            ax.set_title(f"{metric_name} by Algorithm and Task Type")
            
            # Add legend only on the first subplot to avoid repetition
            if i == 0:
                ax.legend(title="Task Type", bbox_to_anchor=(1.05, 1), loc='upper left')
        
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    monitor = SystemMonitor()
    monitor.start()

    try:
        while True:
            print("\nOptions:")
            print("1. Plot task completion and worker stats")
            print("2. Plot response times")
            print("3. Plot schedule timeline")
            print("4. Plot worker loads")
            print("5. Plot algorithm performance")
            print("6. Plot scheduling timeline (new)")
            print("7. Plot algorithm comparison (new)")
            print("8. Exit")

            choice = input("Enter choice: ")

            if choice == "1":
                monitor.plot_task_completion()
            elif choice == "2":
                monitor.plot_response_times()
            elif choice == "3":
                monitor.plot_schedule_timeline()
            elif choice == "4":
                monitor.plot_worker_loads()
            elif choice == "5":
                monitor.plot_algorithm_performance()
            elif choice == "6":
                monitor.plot_scheduling_timeline()
            elif choice == "7":
                monitor.plot_algorithm_comparison()
            elif choice == "8":
                break
            else:
                print("Invalid choice")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        monitor.stop()
