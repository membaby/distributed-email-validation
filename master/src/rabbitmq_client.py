import os

import aio_pika


class RabbitMQClient:
    """
    Small wrapper around aio_pika to keep RabbitMQ-related concerns
    out of the FastAPI app module.
    """

    def __init__(self, connection: aio_pika.RobustConnection) -> None:
        self._connection = connection

    @classmethod
    async def from_env(cls) -> "RabbitMQClient":
        """
        Create a RabbitMQ connection based on environment variables and
        ensure the base queues/exchanges exist.
        """
        rabbitmq_user = os.environ["RABBITMQ_USER"]
        rabbitmq_pass = os.environ["RABBITMQ_PASS"]
        rabbitmq_host = os.environ["RABBITMQ_HOST"]
        rabbitmq_port = os.environ["RABBITMQ_PORT"]
        rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")

        amqp_url = (
            f"amqp://{rabbitmq_user}:{rabbitmq_pass}"
            f"@{rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost}"
        )

        connection = await aio_pika.connect_robust(amqp_url)

        # Create a channel just to declare the shared queues/exchanges.
        channel = await connection.channel()
        await channel.declare_queue("requests_queue", durable=True)
        await channel.declare_exchange("results_exchange", type="direct", durable=True)
        await channel.close()

        return cls(connection)

    async def close(self) -> None:
        if not self._connection.is_closed:
            await self._connection.close()

    async def publish_request(self, email: str) -> None:
        """
        Publish a validation request for the given email.
        """
        channel = await self._connection.channel()
        try:
            await channel.default_exchange.publish(
                aio_pika.Message(body=email.encode("utf-8")),
                routing_key="requests_queue",
            )
        finally:
            await channel.close()