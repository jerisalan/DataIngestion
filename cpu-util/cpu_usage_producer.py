import json
import time
import datetime
import subprocess
from kafka import KafkaProducer

cpu_idle = subprocess.check_output("echo $(vmstat 1 2|tail -1|awk '{print $15}')", shell=True)
cpu_usage = 100 - int(cpu_idle)
ts = time.time()
st = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
print("Time: " + st + " Cpu: " + str(cpu_usage))

producer = KafkaProducer(bootstrap_servers=['152.46.20.245:9092','152.46.19.55:9092','152.1.13.146:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
producer.send('cpu-ip', { 'timestamp': st, 'value': cpu_usage, 'ip', '152.46.19.55'})
producer.close()
