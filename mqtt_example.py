import paho.mqtt.client as mqtt
import time
import threading

# MQTT Broker Settings
BROKER_ADDRESS = "localhost"
PORT = 1883
KEEPALIVE = 60


# Callback functions for client
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT Broker!")
        client.subscribe("test/topic")
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


# Publisher function
def publisher():
    pub_client = setup_client("publisher_1")
    if pub_client:
        pub_client.loop_start()
        count = 0
        while True:
            message = f"Test message {count}"
            result = pub_client.publish("test/topic", message, qos=1)
            print(f"Publishing: {message}")
            count += 1
            time.sleep(2)  # Publish every 2 seconds
        pub_client.loop_stop()


# Subscriber function
def subscriber():
    sub_client = setup_client("subscriber_1")
    if sub_client:
        sub_client.loop_forever()  # Keeps subscriber running


# Main execution
if __name__ == "__main__":

    # Start publisher in a separate thread
    pub_thread = threading.Thread(target=publisher)
    pub_thread.daemon = True
    pub_thread.start()

    # Start subscriber in the main thread
    subscriber()
