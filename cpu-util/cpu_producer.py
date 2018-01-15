import json
import pandas as pd
from kafka import KafkaProducer
from kafka.errors import KafkaError

df = pd.read_csv('realAWSCloudwatch/ec2_cpu_utilization_5f5533.csv')

print(len(df['timestamp']))
print(len(df['value']))

producer = KafkaProducer(bootstrap_servers=['152.46.19.55:9092'], value_serializer=lambda m: json.dumps(m).encode('utf-8'))
for index, row in df.iterrows():
   producer.send('cpu-util', { 'timestamp' : row['timestamp'], 'value' : row['value'] })
   print(row['timestamp'])
