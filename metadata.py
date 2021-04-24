import paho.mqtt.client as mqtt
import json
import base64
import struct
from influxdb import InfluxDBClient

clientinf = InfluxDBClient(host='xxx', port=443, username='xxx', password='xxx', ssl=True, verify_ssl=True, database='gps')

def on_connect(client, userdata, flags, rc):
    client.username_pw_set(username="xxx",password="x")
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    client.subscribe("sprenggps/devices/regnereins/up")


def on_message(client, userdata, msg):
#    print(msg.topic+" "+str(msg.payload))
    data = json.loads(msg.payload)
    payload = base64.b64decode((data["payload_raw"]))
    dev_id = data["dev_id"]
    z=0
    for i in data["metadata"]["gateways"]:
        print(data["metadata"]["gateways"][z]["rssi"])
        gtw_id=data["metadata"]["gateways"][z]["gtw_id"]
        channel=data["metadata"]["gateways"][z]["channel"]
        rssi=data["metadata"]["gateways"][z]["rssi"]
        snr=data["metadata"]["gateways"][z]["snr"]
        json_body = [
           {
                "measurement": "gpsmeta",
                "tags": {
                    "dev_id": dev_id,
                    "gtw_id": gtw_id
                },
                "fields": {
		    "channel": channel,
                    "rssi": rssi
                }
            }
        ]
        clientinf.write_points(json_body)
        z=z+1
        

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

client.connect("eu.thethings.network", 1883, 60)

# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

