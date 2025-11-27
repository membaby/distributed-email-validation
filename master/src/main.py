import asyncio
import os
from typing import Optional

import asyncpg
from fastapi import Depends, FastAPI, HTTPException
from pydantic import BaseModel, EmailStr

from .rabbitmq_client import RabbitMQClient


class ValidateRequest(BaseModel):
    user_id: str
    user_secret: str
    email: EmailStr


class ValidateResponse(BaseModel):
    status: str
    email_address: EmailStr
    is_valid: bool
    message: str


app = FastAPI(title="Distributed Worker Pool Master")


async def _create_db_pool() -> asyncpg.Pool:
    database_host = os.environ["DB_HOST"]
    database_port = os.environ["DB_PORT"]
    database_user = os.environ["DB_USER"]
    database_password = os.environ["DB_PASS"]
    database_name = os.environ["DB_NAME"]
    return await asyncpg.create_pool(
        host=database_host,
        port=int(database_port),
        user=database_user,
        password=database_password,
        database=database_name,
        min_size=1,
        max_size=10,
    )


async def get_db_pool() -> asyncpg.Pool:
    pool: Optional[asyncpg.Pool] = getattr(app.state, "db_pool", None)
    if pool is None:
        raise HTTPException(status_code=500, detail="Database not initialized")
    return pool


async def get_rabbit_client() -> RabbitMQClient:
    client: Optional[RabbitMQClient] = getattr(app.state, "rabbitmq_client", None)
    if client is None:
        raise HTTPException(status_code=500, detail="RabbitMQ not initialized")
    return client


@app.on_event("startup")
async def startup_event() -> None:
    app.state.db_pool = await _create_db_pool()
    app.state.rabbitmq_client = await RabbitMQClient.from_env()


@app.on_event("shutdown")
async def shutdown_event() -> None:
    pool: Optional[asyncpg.Pool] = getattr(app.state, "db_pool", None)
    if pool is not None:
        await pool.close()

    rabbit_client: Optional[RabbitMQClient] = getattr(app.state, "rabbitmq_client", None)
    if rabbit_client is not None:
        await rabbit_client.close()


@app.post("/api/validate", response_model=ValidateResponse)
async def validate_handler(
    payload: ValidateRequest,
    pool: asyncpg.Pool = Depends(get_db_pool),
    rabbit_client: RabbitMQClient = Depends(get_rabbit_client),
) -> ValidateResponse:
    existing = await _fetch_existing_result(pool, payload.email)
    if existing is not None:
        return ValidateResponse(
            status="success",
            email_address=payload.email,
            is_valid=existing["result"] == "1",
            message=existing["additional_info"] or "",
        )

    await rabbit_client.publish_request(payload.email)

    result = await poll_result_from_db(pool, payload.email)
    if result is not None:
        return ValidateResponse(
            status="success",
            email_address=payload.email,
            is_valid=result["result"] == "1",
            message=result["additional_info"] or "",
        )

    return ValidateResponse(
        status="failed",
        email_address=payload.email,
        is_valid=False,
        message="Timeout waiting for result",
    )


async def _fetch_existing_result(pool: asyncpg.Pool, email: str) -> Optional[asyncpg.Record]:
    async with pool.acquire() as connection:
        query = "SELECT result, additional_info FROM results WHERE email_address = $1"
        return await connection.fetchrow(query, email)


async def poll_result_from_db(
    pool: asyncpg.Pool, email: str, timeout_seconds: int = 60
) -> Optional[dict]:
    """
    Poll the database every 200ms for a result. Returns the result dict
    or None if timeout is reached.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_seconds
    poll_interval = 0.2  # 200ms

    while True:
        remaining = deadline - loop.time()
        if remaining <= 0:
            return None

        # Query the database for the result
        async with pool.acquire() as connection:
            query = "SELECT result, additional_info FROM results WHERE email_address = $1"
            row = await connection.fetchrow(query, email)

        if row is not None:
            # Convert asyncpg.Record to dict format
            return {
                "result": row["result"],
                "additional_info": row["additional_info"],
            }

        # Wait 200ms before next poll, but don't exceed the deadline
        sleep_time = min(poll_interval, remaining)
        await asyncio.sleep(sleep_time)


def main() -> None:
    import uvicorn

    uvicorn.run(
        "src.main:app",
        host="0.0.0.0",
        port=5005,
        reload=False,
    )


if __name__ == "__main__":
    main()

