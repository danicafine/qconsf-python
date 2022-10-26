import time
import random
import sys

from classes.user import User 
from helpers import clients,logging

logger = logging.set_logging('user_producer')
config = clients.config()
            

if __name__ == '__main__':
    # set up Kafka Producer for Users
    producer = clients.producer(clients.user_serializer())

    userids = range(5)
    # start 30s production loop
    try:
        while True:
            for userid in userids:
                key = "User_" + str(userid)
                registertime = int(time.time() * 1000)
                regionid = "Region_" + str(random.randrange(15))
                gender = random.choice(['FEMALE', 'MALE', 'OTHER', 'PREFER NOT TO ANSWER'])

                # generate user object
                user = User(registertime, key, regionid, gender)
            
                # send data to Kafka
                print(f"Producing key {key} and value {user.to_dict()}")
                producer.produce(config['topics']['users'], key=str(key), value=user) 

            producer.poll()
            producer.flush()

            time.sleep(5)
    except Exception as e:
        logger.error("Got exception %s", e)
        sys.exit()