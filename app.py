from flask import Flask, request, jsonify
import requests
import psycopg2
import time
import random
import os
from dotenv import load_dotenv
from circuitbreaker import CircuitBreaker, CircuitBreakerError
from circuitbreaker import CircuitBreakerMonitor
import pika

app = Flask(__name__)

def my_fallback_function(arg):
    return "Fallback response"

circuit_breaker = CircuitBreaker(
    failure_threshold=2,
    recovery_timeout=30,
    name="my_circuit",
    fallback_function=my_fallback_function
)

load_dotenv()

# Configure the database connection
conn = psycopg2.connect(
    host=os.getenv('HOST'),
    database=os.getenv('DATABASE'),
    user=os.getenv('USER'),
    password=os.getenv('PASSWORD')
)

monitor = CircuitBreakerMonitor()

def publish_to_queue(anime_id):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='retry_queue')
    channel.basic_publish(exchange='', routing_key='retry_queue', body=str(anime_id))
    connection.close()

# Ruta para obtener detalles de un anime por ID
@app.route('/get_anime/<int:anime_id>', methods=['GET'])
def get_anime(anime_id):
    anime_data = get_anime_details(anime_id)
    if anime_data:
        return jsonify(anime_data)
    else:
        return jsonify({"error": "Anime no encontrado"}), 404

# Function to get anime details from the database
def get_cached_anime_details(anime_id):
    with conn.cursor() as cur:
        select_query = "SELECT * FROM anime WHERE id = %s;"
        cur.execute(select_query, (anime_id,))
        anime_data = cur.fetchone()
        if anime_data:
            selected_data = {
                "anime_id": anime_data[0],
                "title": anime_data[1],
                "title_english": anime_data[2],
                "title_japanese": anime_data[3]
            }
            return selected_data
    return None

# Function to get anime details from the Jikan API and store in the database
@circuit_breaker
def get_anime_details(anime_id):
    # Uncomment this line in order to change the anime_id and work with randoms anime_id
    anime_id = random.randint(1, 1000)

    cached_data = get_cached_anime_details(anime_id)
    if cached_data:
        print('In cache', anime_id)
        return cached_data
    
    url = f'https://api.jikan.moe/v4/anime/{anime_id}/full'
    
    try:
        response = requests.get(url)

        if response.status_code == 200:
            anime_data = response.json()["data"]
            selected_data = {
                "anime_id": anime_id,
                "title": anime_data["title"],
                "title_english": anime_data["title_english"],
                "title_japanese": anime_data["title_japanese"]
            }

            # Insert the data into the database
            with conn.cursor() as cur:
                insert_query = """
                INSERT INTO anime (id, title, title_english, title_japanese)
                VALUES (%(anime_id)s, %(title)s, %(title_english)s, %(title_japanese)s);
                """
                cur.execute(insert_query, selected_data)
                conn.commit()

            return selected_data
        
        elif response.status_code == 429:
            print(f'Maximum number of requests per second reached, status code {response.status_code}')
            publish_to_queue(anime_id)
            raise Exception("Simulated service failure!")

        else:
            print(f'Anime with id {anime_id} not found, status code {response.status_code}')
            publish_to_queue(anime_id)
            raise Exception("Simulated service failure!")
    
    except CircuitBreakerError as e:
        print('except')
        return jsonify({"status": "failure", "error": str(e)})


@app.route('/circuit_state')
def get_circuit_state():
    circuits = monitor.get_circuits()
    circuit_states = []

    for circuit in circuits:
        # Convert last_failure to a string representation
        last_failure_str = str(circuit.last_failure) if circuit.last_failure else None

        circuit_info = {
            "name": circuit.name,
            "state": circuit.state,
            "failure_count": circuit.failure_count,
            "last_failure": last_failure_str
        }

        circuit_states.append(circuit_info)

    return jsonify({"circuits": circuit_states})

if __name__ == '__main__':
    app.run(debug=True)
    
    
    