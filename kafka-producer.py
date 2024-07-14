from kafka import KafkaProducer
import pandas as pd
from json import dumps
from time import sleep

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['15.206.165.54:9092'],  # Change IP and port here
    value_serializer=lambda x: dumps(x).encode('utf-8')
)

# Read the CSV file
df = pd.read_csv("indexProcessed.csv")

# Display the first few rows of the DataFrame
print(df.head())

# Loop indefinitely
while True:
    # Sample one row randomly from the DataFrame and convert it to a dictionary
    dict_stock = df.sample(1).to_dict(orient="records")[0]
    
    # Send the sampled data to Kafka topic named 'demo1'
    producer.send('demo1', value=dict_stock)
    
    # Wait for 1 second before sending the next message
    sleep(1)
    
    # Flush the producer's message queue
    # producer.flush()
