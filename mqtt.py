import paho.mqtt.client as mqtt
import json
import time
from datetime import datetime, timedelta


# Function to read SCADA data from a file
def read_scada_data(file_path, last_position=0):
    data = []
    current_position = last_position

    with open(file_path, 'r') as file:
        file.seek(current_position)
        for line in file:
            time_offset, user, computer = line.strip().split(',')
            data.append({
                "timestamp": int(time_offset),
                "user": user,
                "computer": computer
            })

    return data, current_position

# MQTT broker settings
broker_address = "127.0.0.1" #localhost
broker_port = 1883 #listening port
topic = "scada" #subscribe the other receiver client to this topic

# Create MQTT client
client = mqtt.Client()

# Connect to the broker
client.connect(broker_address, broker_port)

# File path for the SCADA data
file_path = "scada.txt"

try:
    while True:
        # Read new SCADA data from the file
        new_data, last_position = read_scada_data(file_path)

        if new_data:
            # Process and publish each new data point
            for event in new_data:
                # Convert data to JSON
                payload = json.dumps(event)

                # Publish data to the MQTT broker
                client.publish(topic, payload)
                print(f"Published: {payload}")

                # Wait for 1 minutes
                time.sleep(60) #publishes new data every minute
        else:
            print("No new data to publish")

except KeyboardInterrupt:
    print("Script terminated by user")
except FileNotFoundError:
    print(f"Error: File '{file_path}' not found")
finally:
    # Disconnect from the broker
    client.disconnect()
    print("Disconnected from broker")
