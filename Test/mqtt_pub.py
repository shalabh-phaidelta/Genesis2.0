# python 3.6

import random
import time
import json


from paho.mqtt import client as mqtt_client


broker = 'phaidelta.com'
port = 1883
topic = "test/Genesis"
# generate client ID with pub prefix randomly
# client_id = f'python-mqtt-{random.randint(0, 1000)}'
# username = 'emqx'
# password = 'public'

def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %d\n", rc)

    # client = mqtt_client.Client(client_id)
    client = mqtt_client.Client()
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client):
    while True:
        time.sleep(1)

        with open('data.JSON') as json_file:
            data = json.load(json_file)
            print(type(data), data)
            msg = json.dumps(data) #data
            result = client.publish(topic, msg)
            print(result)
        break

def run():
    client = connect_mqtt()
    client.loop_start()
    publish(client)


if __name__ == '__main__':
    run()