import asyncio
import os
from typing import Optional

import aio_pika
import asyncpg
from .EmailValidator import EmailValidator
import socket


class Worker:
    def __init__(self):
        self.email_validator = EmailValidator()
        self.rabbitmq_connection: Optional[aio_pika.RobustConnection] = None
        self.db_connection: Optional[asyncpg.Connection] = None
        self.checked_by = socket.gethostname()

    async def _connect_db(self) -> asyncpg.Connection:
        """Create a single database connection."""
        database_host = os.environ["DB_HOST"]
        database_port = os.environ["DB_PORT"]
        database_user = os.environ["DB_USER"]
        database_password = os.environ["DB_PASS"]
        database_name = os.environ["DB_NAME"]
        return await asyncpg.connect(
            host=database_host,
            port=int(database_port),
            user=database_user,
            password=database_password,
            database=database_name,
        )

    async def _connect_rabbitmq(self) -> aio_pika.RobustConnection:
        """Connect to RabbitMQ and return the connection."""
        rabbitmq_user = os.environ["RABBITMQ_USER"]
        rabbitmq_pass = os.environ["RABBITMQ_PASS"]
        rabbitmq_host = os.environ["RABBITMQ_HOST"]
        rabbitmq_port = os.environ["RABBITMQ_PORT"]
        rabbitmq_vhost = os.environ.get("RABBITMQ_VHOST", "/")

        amqp_url = (
            f"amqp://{rabbitmq_user}:{rabbitmq_pass}"
            f"@{rabbitmq_host}:{rabbitmq_port}/{rabbitmq_vhost}"
        )
        return await aio_pika.connect_robust(amqp_url)

    async def _save_result_to_db(
        self, email: str, is_valid: bool, message: str
    ) -> None:
        """Save validation result to the database."""
        db_connection = await self._connect_db()
        result_value = "1" if is_valid else "0"
        query = """
            INSERT INTO results (email_address, result, additional_info, checked_by)
            VALUES ($1, $2, $3, $4)
        """
        await db_connection.execute(query, email, result_value, message, self.checked_by)
        await db_connection.close()


    async def _process_message(self, message: aio_pika.IncomingMessage) -> None:
        """Process a single message from the queue."""
        async with message.process():
            try:
                email = message.body.decode("utf-8").strip()
                print(f"Processing email: {email}")

                # Validate the email using EmailValidator (run in executor to avoid blocking)
                loop = asyncio.get_event_loop()
                email_address, is_valid, validation_message = await loop.run_in_executor(
                    None, self.email_validator.validate, email
                )

                # Save result to database
                await self._save_result_to_db(email_address, is_valid, validation_message)
                print(f"Processed {email_address}: valid={is_valid}, message={validation_message}")

            except Exception as e:
                print(f"Error processing message: {e}")
                # Message will be rejected and potentially requeued
                raise

    async def start(self) -> None:
        self.rabbitmq_connection = await self._connect_rabbitmq()
        print("Connected to RabbitMQ")

        channel = await self.rabbitmq_connection.channel()
        await channel.set_qos(prefetch_count=1)
        queue = await channel.declare_queue("requests_queue", durable=True)

        print("Worker started, waiting for messages...")

        await queue.consume(self._process_message)

        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            print("Shutting down worker...")

    async def stop(self) -> None:
        """Clean up connections."""
        if self.rabbitmq_connection and not self.rabbitmq_connection.is_closed:
            await self.rabbitmq_connection.close()
        if self.db_connection and not self.db_connection.is_closed:
            await self.db_connection.close()


async def main() -> None:
    worker = Worker()
    try:
        await worker.start()
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
