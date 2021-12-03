from ProducerKafka import ProducerKafka
import logging
import time
import os
from logging.config import dictConfig

from test import MyTest

class Main(object):

    def __init__(self):
        if 'KAFKA_BROKERS' in os.environ:
            kafka_brokers = os.environ['KAFKA_BROKERS'].split(',')
        else:
            raise ValueError('KAFKA_BROKERS environment variable not set')

        logging_config = dict(
            version=1,
            formatters={
                'f': {'format':
                      '%(asctime)s %(name)-12s %(levelname)-8s %(message)s'}
            },
            handlers={
                'h': {'class': 'logging.StreamHandler',
                      'formatter': 'f',
                      'level': logging.DEBUG}
            },
            root={
                'handlers': ['h'],
                'level': logging.DEBUG,
            },
        )
        self.logger = logging.getLogger()

        dictConfig(logging_config)
        self.logger.info("Initializing Kafka Producer")
        self.logger.info("KAFKA_BROKERS={0}".format(kafka_brokers))
        self.mytest = MyTest(kafka_brokers)
        self.producerKafka = ProducerKafka(kafka_brokers)
        self.logger.info("Initialized Kafka Producer")

    def run(self):
        starttime = time.time()
        self.logger.info("Complete")

if __name__ == "__main__":
    logging.info("Initializing")
    main = Main()
    main.run()