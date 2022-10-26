import time
import random

from classes.user import User 
from classes.usermask import UserMask
from helpers import clients,logging

from confluent_kafka.error import SerializationError

logger = logging.set_logging('user_streaming')
config = clients.config()
    

if __name__ == '__main__':
    # set up Kafka Consumer for Users
    consumer = clients.consumer(clients.user_deserializer(), 'consumer-group-usermask', [config['topics']['users']])
    
    # set up Kafka Producer for UserMask
    producer = clients.producer(clients.usermask_serializer())

    # start consumption loop
    try:
        while True:
            msg = consumer.poll(1.0)

            if msg is None:
                logger.info("Did not fetch a message.")
            else:
                # received a message
                user = msg.value()
                print(f"Consuming key {user.userid} and value {user.to_dict()}")

                # generate usermask object
                usermask = UserMask(user.registertime, user.userid, user.regionid)
            
                # send data to Kafka
                print(f"Producing key {user.userid} and value {usermask.to_dict()}")
                producer.produce(config['topics']['usermasks'], key=str(user.userid), value=usermask) 

                producer.poll()
                producer.flush()

            time.sleep(5)
    except SerializationError as e:
        # report malformed record, discard results, continue polling 
        logger.error("Message deserialization failed %s", e)
        raise    
    except Exception as e:
        logger.error("Got other exception %s", e)     
    finally:
        consumer.close()
