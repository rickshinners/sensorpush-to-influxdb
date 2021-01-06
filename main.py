#!/usr/bin/python3

import logging
import os
import sched
import sys
import time
from influxdb import InfluxDBClient

from pysensorpush import PySensorPush

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

sensorpush_username = os.getenv('SENSORPUSH_USERNAME', None)
sensorpush_password = os.getenv('SENSORPUSH_PASSWORD', None)
if (sensorpush_username is None) or (sensorpush_password is None):
    logger.error("Must define env variables SENSORPUSH_USERNAME and SENSORPUSH_PASSWORD")
    raise SystemExit
sensorpush = PySensorPush(sensorpush_username, sensorpush_password)

scheduler = sched.scheduler(time.time, time.sleep)

sensorpush_poll_seconds = os.getenv("SENSORPUSH_POLL_SECONDS", 60)

influxdb_client = InfluxDBClient(
    host=os.getenv('INFLUXDB_HOST', 'localhost'),
    port=int(os.getenv('INFLUXDB_PORT', 8086)),
    username=os.getenv('INFLUXDB_USERNAME', None),
    password=os.getenv('INFLUXDB_PASSWORD', None),
    ssl=bool(os.getenv('INFLUXDB_SSL', False)),
    verify_ssl=bool(os.getenv('INFLUXDB_VERIFY_SSL', False))
)
influxdb_client.create_database(os.getenv('INFLUXDB_DATABASE', 'sensorpush'))
influxdb_client.switch_database(os.getenv('INFLUXDB_DATABASE', 'sensorpush'))


def polling_loop():
    points = []
    samples = sensorpush.samples
    for sensor_id in samples['sensors']:
        if sensor_id not in sensorpush.sensors:
            sensorpush.update(update_sensors=True)
        sensor_name = sensorpush.sensors[sensor_id]['name']
        for sensor_reading in samples['sensors'][sensor_id]:
            observed = sensor_reading['observed']
            temperature = sensor_reading['temperature']
            humidity = sensor_reading['humidity']
            points.append({
                "measurement": "sensor",
                "tags": {
                    "sensor_id": sensor_id,
                    "sensor_name": sensor_name
                },
                "time": observed,
                "fields": {
                    "temperature": float(temperature),
                    "humidity": float(humidity)
                }
            })
    logger.debug("Writing sensor measurements to influxdb")
    influxdb_client.write_points(points)
    scheduler.enter(sensorpush_poll_seconds, 1, polling_loop)


def main():
    scheduler.enter(0, 1, polling_loop)
    try:
        scheduler.run()
    except KeyboardInterrupt:
        raise SystemExit
    ##TODO: try reconnecting if influx server or sensorpush exception (ConnectionError)


if __name__ == "__main__":
    main()
