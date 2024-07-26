from flask import Flask, jsonify, request
from confluent_kafka import Producer
import json
import requests

app = Flask(__name__)

# Kafka configuration
conf = {
    'bootstrap.servers': 'localhost:29092'  # Kafka broker address
}

# Create a Producer instance
producer = Producer(conf)

# Define the topic to produce to
topic = 'mytopic'

@app.route('/hello')
def home():
    return "Welcome to the Flask API!"

@app.route('/api/data', methods=['GET'])
def get_data():
    data = {
        'name': 'John Doe',
        'age': 30,
        'occupation': 'Engineer'
    }
    return jsonify(data)

@app.route('/api/data', methods=['POST'])
def post_data():
    data = request.json
    return jsonify(data), 201

def trigger_dag(base_endpoint, dag_id):
    url = f"{base_endpoint}/api/v1/dags/{dag_id}/dagRuns"
    headers = {
        'Content-Type': 'application/json'
    }
    data = {
        'conf': {}
    }
    
    response = requests.post(url, headers=headers, data=json.dumps(data), auth=('airflow', 'airflow'))
    
    if response.status_code == 200:
        print("DAG triggered successfully!")
    else:
        print(f"Failed to trigger DAG: {response.status_code} - {response.text}")
    
    return response.json()

def on_delivery(err, msg):
    """Called once for each message produced to indicate delivery result.
    Triggered by poll() or flush()."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")
        #make api to trigger pyspark dag
        print(f'value: {msg.value().decode()}')
        #trigger_dag('http://127.0.0.1:8080', 'pyspark_example')
        
@app.route('/api/kafka', methods=['POST'])
def post_to_kafka():
    data = request.json
    producer.produce(topic, value=json.dumps(data), callback=on_delivery)
    producer.poll(0)
    producer.flush()
    return jsonify({'message': 'Data sent to Kafka'}), 201

if __name__ == '__main__':
    # Please do not set debug=True in production
    app.run(host="0.0.0.0", port=5000, debug=True)
