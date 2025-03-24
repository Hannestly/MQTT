# monitor.py
import paho.mqtt.client as mqtt
import time
import json
import matplotlib.pyplot as plt
from collections import defaultdict


class SystemMonitor:
    def __init__(self, broker_host="localhost", broker_port=1883):
        self.broker_host = broker_host
        self.broker_port = broker_port
        self.mqtt_client = mqtt.Client(client_id="monitor")
        self.task_history = []
        self.schedule_history = []
        self.client_status = defaultdict(dict)

        # Setup MQTT callbacks
        self.mqtt_client.on_connect = self.on_connect
        self.mqtt_client.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc):
        print(f"Monitor connected with result code {rc}")
        # Subscribe to relevant topics
        client.subscribe("task/status")
        client.subscribe("system/schedule_history")
        client.subscribe("client/+/task_history")

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        payload = msg.payload.decode()

        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            print(f"Invalid JSON received on {topic}")
            return

        if topic == "task/status":
            self.task_history.append(data)
        elif topic == "system/schedule_history":
            # Schedule history from broker
            if isinstance(data, list):
                self.schedule_history.extend(data)
        elif topic.startswith("client/") and topic.endswith("/task_history"):
            # Task history from a client
            client_id = topic.split("/")[1]
            if isinstance(data, list):
                self.client_status[client_id]["task_history"] = data

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

    def plot_task_completion(self):
        """Plot task completion statistics"""
        if not self.task_history:
            print("No task history to plot")
            return

        # Group by algorithm
        algorithms = defaultdict(lambda: {"completed": 0, "missed": 0})
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            if task["status"] == "completed":
                algorithms[alg]["completed"] += 1
            else:
                algorithms[alg]["missed"] += 1

        # Prepare data for plotting
        algs = list(algorithms.keys())
        completed = [algorithms[alg]["completed"] for alg in algs]
        missed = [algorithms[alg]["missed"] for alg in algs]

        # Plot
        plt.figure(figsize=(10, 6))
        bar_width = 0.35
        index = range(len(algs))

        plt.bar(index, completed, bar_width, label="Completed")
        plt.bar(index, missed, bar_width, bottom=completed, label="Missed")

        plt.xlabel("Scheduling Algorithm")
        plt.ylabel("Number of Tasks")
        plt.title("Task Completion by Scheduling Algorithm")
        plt.xticks(index, algs)
        plt.legend()
        plt.tight_layout()
        plt.show()

    def plot_response_times(self):
        """Plot task response times"""
        if not self.task_history:
            print("No task history to plot")
            return

        # Group by algorithm
        algorithms = defaultdict(list)
        for task in self.task_history:
            alg = task.get("algorithm", "unknown")
            algorithms[alg].append(task["response_time"])

        # Prepare data for plotting
        algs = list(algorithms.keys())
        avg_response = [sum(times) / len(times) for times in algorithms.values()]

        # Plot
        plt.figure(figsize=(10, 6))
        plt.bar(algs, avg_response)

        plt.xlabel("Scheduling Algorithm")
        plt.ylabel("Average Response Time (s)")
        plt.title("Average Task Response Time by Scheduling Algorithm")
        plt.tight_layout()
        plt.show()

    def plot_schedule_timeline(self):
        """Plot a timeline of scheduled tasks"""
        if not self.schedule_history:
            print("No schedule history to plot")
            return

        # Group by algorithm and time
        timeline = defaultdict(list)
        for event in self.schedule_history:
            timeline[event["algorithm"]].append(
                (event["time"], event["task"]["task_id"])
            )

        # Prepare data for plotting
        plt.figure(figsize=(12, 6))
        colors = {"RM": "blue", "EDF": "green", "RR": "red"}

        for alg, events in timeline.items():
            times = [e[0] - min(e[0] for e in events) for e in events]  # Relative time
            tasks = [e[1] for e in events]
            plt.scatter(
                times, [alg] * len(times), label=alg, color=colors.get(alg, "gray")
            )

            # Add task IDs as annotations
            for i, task_id in enumerate(tasks):
                plt.annotate(
                    task_id,
                    (times[i], alg),
                    textcoords="offset points",
                    xytext=(0, 10),
                    ha="center",
                )

        plt.xlabel("Time (s)")
        plt.ylabel("Scheduling Algorithm")
        plt.title("Task Scheduling Timeline")
        plt.yticks(list(timeline.keys()))
        plt.legend()
        plt.tight_layout()
        plt.show()


if __name__ == "__main__":
    monitor = SystemMonitor()
    monitor.start()

    try:
        while True:
            print("\nOptions:")
            print("1. Plot task completion")
            print("2. Plot response times")
            print("3. Plot schedule timeline")
            print("4. Exit")

            choice = input("Enter choice: ")

            if choice == "1":
                monitor.plot_task_completion()
            elif choice == "2":
                monitor.plot_response_times()
            elif choice == "3":
                monitor.plot_schedule_timeline()
            elif choice == "4":
                break
            else:
                print("Invalid choice")

    except KeyboardInterrupt:
        print("\nShutting down...")
    finally:
        monitor.stop()
