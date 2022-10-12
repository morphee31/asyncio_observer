import asyncio

import aiormq

from loguru import logger


class Rabbitmq:

    def __init__(self, config: dict):
        self._config = config["rabbitmq"]
        self._username = self._config["username"]
        self._password = self._config["password"]
        self._host = self._config["host"]
        self._port = self._config["port"]
        self._rabbitmq_connection: aiormq.Connection = None
        self._rabbitmq_channel: aiormq.Channel = None

    async def run(self):
        await self._create_connection()
        await self._set_channel()
        await self._declare_exchange()

    async def start_client(self):
        self._rabbitmq_connection = await aiormq.connect(
            f"amqp://{self._username}:{self._password}@{self._host}:{self._port}/")
        self._rabbitmq_channel = await self._rabbitmq_connection.channel()
        await self._rabbitmq_channel.exchange_declare(
            exchange=self._config["exchange"],
            exchange_type=self._config.get("exchange_type", "direct")
        )

    async def _create_connection(self):
        try:
            self._rabbitmq_connection = await aiormq.connect(f"amqp://{self._username}:{self._password}@{self._host}:{self._port}/")
        except Exception as e:
            logger.exception(e)

    async def _set_channel(self):
        self._rabbitmq_channel = await self._rabbitmq_connection.channel()

    async def _declare_exchange(self):
        await self._rabbitmq_channel.exchange_declare(
            exchange=self._config["exchange"],
            exchange_type=self._config.get("exchange_type", "direct")
        )

    async def publish_message(self, message):
        await self._rabbitmq_channel.basic_publish(
            body=message,
            exchange=self._config["exchange"],
            routing_key=self._config['routing_key']
        )


async def main():
    config = {
        "rabbitmq": {
            "host": "broker",
            "port": 5672,
            "username": "user",
            "password": "password",
            "exchange": "asyncio_observer",
            "routing_key": "observer"
        },
        "observer": {
            "watched_folder": "/data/in",
            "buf_size": 1024
        }
    }
    await Rabbitmq(config=config).run()



if __name__ == '__main__':
    asyncio.run(main(), debug=True)

"""
async def main():
    # Perform connection
    connection = await aiormq.connect("amqp://guest:guest@localhost/")

    # Creating a channel
    channel = await connection.channel()

    await channel.exchange_declare(
        exchange='logs', exchange_type='fanout'
    )

    body = b' '.join(sys.argv[1:]) or b"Hello World!"

    # Sending the message
    await channel.basic_publish(
        body, routing_key='info', exchange='logs'
    )

    print(" [x] Sent %r" % (body,))

    await connection.close()


loop = asyncio.get_event_loop()
loop.run_until_complete(main())"""
