import paho.mqtt.client as mqtt
import time
import threading
import json

# MQTT Broker Settings
BROKER_ADDRESS = "localhost"
PORT = 1883
KEEPALIVE = 60


# Callback functions for client
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        # Subscribe to relevant topics
        client.subscribe("client/+/registered")
        client.subscribe("task/+/ack")
        client.subscribe("client/+/task")
        client.subscribe("system/schedule_history")
    else:
        print(f"Connection failed with code {rc}")


def on_message(client, userdata, msg):
    print(f"Received message: {msg.payload.decode()} on topic {msg.topic}")


def on_publish(client, userdata, mid):
    print(f"Message {mid} published")


def on_subscribe(client, userdata, mid, granted_qos):
    print(f"Subscribed to topic with QoS: {granted_qos}")


# Client setup
def setup_client(client_id):
    client = mqtt.Client(client_id=client_id)
    client.on_connect = on_connect
    client.on_message = on_message
    client.on_publish = on_publish
    client.on_subscribe = on_subscribe

    try:
        client.connect(BROKER_ADDRESS, PORT, KEEPALIVE)
        return client
    except Exception as e:
        print(f"Error connecting to broker: {e}")
        return None


# Publisher function - now sends task-related messages
def publisher():
    pub_client = setup_client("test_client_1")
    if pub_client:
        pub_client.loop_start()
        
        # First register the client
        pub_client.publish("client/register", 
            json.dumps({"client_id": "test_client_1"}))
        time.sleep(1)  # Wait for registration
        
        # Request schedule history before starting tasks
        pub_client.publish("system/control", 
            json.dumps({"command": "get_schedule_history"}))
        
        # Submit test tasks
        for i in range(5):
            # Submit task
            task = {
                "task_id": f"task_{i}",
                "client_id": "test_client_1",
                "computation_time": 1,
                "period": 5,
                "deadline": 5,
                "scheduling": "RM"  # Try with "RM", "EDF", or "RR"
            }
            pub_client.publish("task/submit", json.dumps(task))
            print(f"Submitting task_{i}")
            
            # Simulate task completion after 2 seconds
            time.sleep(2)
            status = {
                "task_id": f"task_{i}",
                "client_id": "test_client_1",
                "status": "completed",
                "response_time": 1.5,
                "algorithm": "RM"
            }
            pub_client.publish("task/status", json.dumps(status))
            print(f"Completing task_{i}")
            
            # Request updated schedule history after each task
            pub_client.publish("system/control", 
                json.dumps({"command": "get_schedule_history"}))
            
            time.sleep(1)  # Wait before next task
            
        # Request final schedule history
        pub_client.publish("system/control", 
            json.dumps({"command": "get_schedule_history"}))
        time.sleep(1)  # Wait for final history
        
        pub_client.loop_stop()


# Subscriber function - monitors responses
def subscriber():
    sub_client = setup_client("monitor_client_1")
    if sub_client:
        sub_client.loop_forever()  # Keeps subscriber running


# Main execution
if __name__ == "__main__":
    print("Starting test client...")
    print("Will send 5 tasks and then exit in 10 seconds...")
    
    # Start publisher in a separate thread
    pub_thread = threading.Thread(target=publisher)
    pub_thread.daemon = True
    pub_thread.start()

    # Give enough time for tasks to complete
    time.sleep(20)  # Wait for all 5 tasks (each takes ~3 seconds) plus some buffer
    print("All tasks completed, shutting down...")
    exit(0)
