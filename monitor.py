# monitor.py
import paho.mqtt.client as mqtt
import time
import json
import matplotlib.pyplot as plt
from collections import defaultdict
import numpy as np


class SystemMonitor:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="monitor")
        self.task_history = []
        self.schedule_history = []
        self.worker_status = defaultdict(dict)
        self.worker_history = defaultdict(list)

        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print(f"Monitor connected with result code {rc}")
        # Subscribe to relevant topics
        client.subscribe("task/status")
        client.subscribe("system/schedule_history")
        client.subscribe("worker/+/task_history")
        client.subscribe("worker/heartbeat")  # New subscription for worker heartbeats

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
            # Only print non-heartbeat messages
            if topic != "worker/heartbeat":
                print(f"\nMonitor received message on {topic}:")
                print(f"  Data: {data}")
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic == "task/status":
            print(f"Monitor: Recording task completion - Task {data['task_id']}")
            self.task_history.append(data)
        elif topic == "system/schedule_history":
            if isinstance(data, list):
                self.schedule_history.extend(data)
        elif topic == "worker/heartbeat":
            self.update_worker_status(data)  # Still update status but don't print
        elif topic.startswith("worker/") and topic.endswith("/task_history"):
            worker_id = topic.split("/")[1]
            if isinstance(data, list):
                self.worker_history[worker_id].extend(data)

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
            print("6. Exit")

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
                break
            else:
                print("Invalid choice")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        monitor.stop()
