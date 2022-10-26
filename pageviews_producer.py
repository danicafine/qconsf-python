import time
import random
import sys

from classes.pageview import Pageview 
from helpers import clients,logging


logger = logging.set_logging('pageviews_producer')
config = clients.config()
            

if __name__ == '__main__':
    # set up Kafka Producer for Pageviewss
    producer = clients.producer(clients.pageview_serializer())

    pageids = range(5)
    # start 30s production loop
    try:
        while True:
            for pageid in pageids:

        #                self.viewtime = viewtime
        #                self.userid   = userid
        # self.pageid   = pageid

                key = "Pageviews_" + str(pageid)
                viewtime = int(time.time() * 1000)
                userid = "UserId_" + str(random.randrange(5))
    

                # generate pageview object
                pageview = Pageview(viewtime, key, userid)
            
                # send data to Kafka
                print(f"Producing key {key} and value {pageview.to_dict()}")
                producer.produce(config['topics']['pageviews'], key=str(key), value=pageview) 

            producer.poll()
            producer.flush()

            time.sleep(5)
    except Exception as e:
        logger.error("Got exception %s", e)
        sys.exit()