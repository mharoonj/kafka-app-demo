from data import get_registered_user
from ensurepip import bootstrap
from kafka import KafkaProducer
import json
import time 

def json_serializer(data):
    return json.dumps(data).encode("utf-8")


# creating instance
producer = KafkaProducer(bootstrap_servers=['localhost:9092'], #need to connect to broker/kafka-server
                        value_serializer=json_serializer
) 

if __name__ == "__main__":
   while 1==1:
       registered_user = get_registered_user() 
       print(f"Registered User data : {registered_user}")
       producer.send("registered_user", registered_user) #topic name , values    ------ values will be automatically serialized because we added serializer at top
       time.sleep(4)