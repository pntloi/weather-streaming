import time 
import json
import requests
from config import config
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
import datetime

bootstrap_servers = 'localhost:9092'
topic_names = ['openweather']

def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def create_topic(topic_names):
    client = KafkaAdminClient(
        bootstrap_servers="localhost:9092"
    )
    broker_topics = client.list_topics()

    for topic_name in topic_names:
        if topic_name not in broker_topics:
            topic_name = [NewTopic(topic_name, num_partitions=2, replication_factor=1)]
            client.create_topics(topic_name)
            print('The topic \'{}\' created'.format(topic_name))
        else:
            print('The topic \'{}\' exists'.format(topic_name))



def get_weather_info(openweather_endpoint:str) -> dict:
    '''
    Request data from openweather api

    Params: openweather_endpoint - str:
        OpenWeather API endpoint

    Returns:
        json_msg: return the message to send to kafka
    '''
    response = requests.get(openweather_endpoint)
    json_data = response.json()
    city_id = json_data['id']
    city_name = json_data['name']
    lat = json_data['coord']['lat']
    lon = json_data['coord']['lon']
    country = json_data['sys']['country']
    temp = json_data['main']['temp']
    max_temp = json_data['main']['temp_max']
    min_temp = json_data['main']['temp_min']
    feels_like = json_data['main']['feels_like']
    humidity = json_data['main']['humidity']


    json_msg = {
        'created_at': time.strftime('%Y-%m-%d %H:%M:%S'),
        'city_id': city_id,
        'city_name': city_name,
        'lat': lat,
        'lon': lon,
        'country': country,
        'temp': temp,
        'max_temp': max_temp,
        'min_temp': min_temp,
        'feels_like': feels_like,
        'humidity': humidity
    }

    return json_msg

def main():
    create_topic(topic_names)
    kafka_topic = 'openweather'
    api_key = config()
    cities = ('London', 'Berlin', 'Paris', 'Barcelona', 'Amsterdam', 'Krakow', 'Vienna')
    producer = kafka_producer()
    while True:
        for city in cities:
            openweather_endpoint = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric'
            json_msg = get_weather_info(openweather_endpoint)
            producer.send(kafka_topic, json_msg)
            print(f'Published {city}: {json.dumps(json_msg)}')
            sleep = 5
            print(f'Waiting {sleep} seconds ...')
            time.sleep(sleep)


if __name__=="__main__":
    main()



