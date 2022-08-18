import json
import random

from paho.mqtt import client as mqtt_client

import data_access as db
from shared import logger as log

#  134.209.158.162 -t 'test/topic'

# broker = 'phaidelta.com'
broker = 'phaidelta.com'
port = 1883
topic = "Genesis"
# 'IOT/Genesis/'
# generate client ID with pub prefix randomly
# client_id = f'python-mqtt-{random.randint(0, 100)}'
# username = 'emqx'
# password = 'public'


def connect_mqtt() -> mqtt_client:
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            log.logger("Connected to MQTT Broker!")
        else:
            log.logger("Failed to connect, return code %d\n", rc)

    # client = mqtt_client.Client(client_id)
    client = mqtt_client.Client()
    # client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def subscribe(client: mqtt_client):
    def on_message(client, userdata, msg):
        data = json.loads(msg.payload.decode())
        log.logger("payload recived")
        db.recive_data_from_aliter(data)
    client.subscribe(topic)
    client.on_message = on_message


def run():
    client = connect_mqtt()
    subscribe(client)
    client.loop_forever()



if __name__ == '__main__':
    run()