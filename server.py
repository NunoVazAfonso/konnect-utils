import tornado.ioloop
import tornado.web
from pathlib import Path
import logging
import json

import confluent_kafka

from consumer import KafkaConsumer

logger = logging.getLogger(__name__)
WEB_SERVER_PORT = 8889


class MainHandler(tornado.web.RequestHandler):

    # fetch the Template
    template_dir = tornado.template.Loader(f"{Path(__file__).parents[0]}/templates")
    html_template = template_dir.load("index.html")

    def initialize(self, TestModel):
        """Initializes the handler configuration"""
        self.TestModel = TestModel

    def get(self):
        """ writes out the template with associated models """
        self.write(
            MainHandler.html_template.generate( 
                TestModel=self.TestModel 
            )
        )


class LocalTestModel:
    """Defines the Weather model"""

    def __init__(self):
        """Creates the test model"""
        print("init model")
        self.test_attr = "test attr"
        self.message_ids = {}

    def process_message(self, message):
        """Handles incoming data"""
        print("creating message")
        self.msg = json.loads(message.value())

        print(self.message_ids)

        if self.msg["stop_id"] not in self.message_ids: 
            self.message_ids[self.msg["stop_id"]] = self.msg

local_model = LocalTestModel()

def make_app():
    """ return application object with rendered html and associated models """
    return tornado.web.Application([
        (
            r"/", 
            MainHandler, 
            {"TestModel": local_model} # this is going to be passed to initialize() function
        ),
    ])


if __name__ == "__main__":

    app = make_app()

    app.listen(WEB_SERVER_PORT)

    consumers = [
        KafkaConsumer(
            "com.nva.pg.0709.stations", # topic name pattern
            local_model.process_message
        )
    ]

    try:
        logger.info(
            f"Open a web browser to http://localhost:{WEB_SERVER_PORT} to see the Transit Status Page"
        )
        for consumer in consumers:
            tornado.ioloop.IOLoop.current().spawn_callback(consumer.consume)

        tornado.ioloop.IOLoop.current().start()

    except KeyboardInterrupt as e:
        logger.info("shutting down server")
        tornado.ioloop.IOLoop.current().stop()
        for consumer in consumers:
            consumer.close()