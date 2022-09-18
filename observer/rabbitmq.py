import sys
import asyncio
import aiormq

class Rabbitmq:

    def __init__(self, config:dict):
        self._config = config["rabbitmq"]
        self._username = self._config["username"]
        self._password = self._config["password"]
        self._host = self._config["host"]
        self._port = self._config["port"]
        self._connection : aiormq.Connection = None
        self._channel: aiormq.Channel = None

    async def _create_connection(self):
        self._connection = aiormq.connect(f"amqp://{self._username}:{self._password}@{self._host}:{self._port}")

    async def _set_channel(self):
        self._channel = await self._connection.channel()

    async def declare_exchange(self):
        self._channel.exchange_declare(exchange=self._config)

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
loop.run_until_complete(main())