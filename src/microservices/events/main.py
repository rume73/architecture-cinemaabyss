import os
import json
import logging
import uuid
from datetime import datetime
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from kafka import KafkaProducer, KafkaConsumer
import threading
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="CinemaAbyss Events Service")

KAFKA_BROKERS = os.getenv("KAFKA_BROKERS", "kafka:9092")
PORT = int(os.getenv("PORT", "8082"))

producer = None
consumer = None
consumer_thread = None
running = True


def get_kafka_producer():
    global producer
    if producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                acks='all',
                retries=3
            )
            logger.info(f"Kafka producer connected to {KAFKA_BROKERS}")
        except Exception as e:
            logger.error(f"Failed to create Kafka producer: {e}")
            producer = None
    return producer


def get_kafka_consumer():
    global consumer
    if consumer is None:
        try:
            consumer = KafkaConsumer(
                'movie-events',
                'user-events',
                'payment-events',
                bootstrap_servers=KAFKA_BROKERS,
                auto_offset_reset='earliest',
                enable_auto_commit=True,
                group_id='events-service-group',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                consumer_timeout_ms=1000
            )
            logger.info(f"Kafka consumer connected to {KAFKA_BROKERS}")
        except Exception as e:
            logger.error(f"Failed to create Kafka consumer: {e}")
            consumer = None
    return consumer


def consume_messages():
    global running, consumer

    logger.info("Starting Kafka consumer thread...")

    max_retries = 10
    retry_delay = 5

    for i in range(max_retries):
        try:
            consumer = get_kafka_consumer()
            if consumer:
                break
        except Exception as e:
            logger.warning(f"Kafka not ready (attempt {i + 1}/{max_retries}): {e}")
            time.sleep(retry_delay)

    if not consumer:
        logger.error("Failed to connect to Kafka after retries")
        return

    logger.info("Kafka consumer started, listening for events...")

    while running:
        try:
            messages = consumer.poll(timeout_ms=1000)

            for topic, msgs in messages.items():
                for msg in msgs:
                    event_data = msg.value
                    logger.info(f"Received event from {topic}: {json.dumps(event_data, indent=2)}")
                    process_event(topic, event_data)

        except Exception as e:
            if running:
                logger.error(f"Error consuming messages: {e}")
            time.sleep(1)


def process_event(topic: str, event_data: dict):
    event_type = event_data.get('type', 'unknown')
    event_id = event_data.get('id', 'unknown')

    logger.info(f"Processing {topic} event: {event_type} (ID: {event_id})")

    if 'movie' in topic.lower():
        logger.info(f"Movie event: {event_data.get('data', {}).get('title', 'N/A')}")
    elif 'user' in topic.lower():
        logger.info(f"User event: {event_data.get('data', {}).get('username', 'N/A')}")
    elif 'payment' in topic.lower():
        logger.info(f"Payment event: {event_data.get('data', {}).get('amount', 'N/A')}")


@app.on_event("startup")
async def startup_event():
    global consumer_thread
    logger.info("Starting Events Service...")
    consumer_thread = threading.Thread(target=consume_messages, daemon=True)
    consumer_thread.start()
    logger.info("Events Service started successfully")


@app.on_event("shutdown")
async def shutdown_event():
    global running, producer, consumer
    logger.info("Shutting down Events Service...")
    running = False
    if producer:
        producer.close()
    if consumer:
        consumer.close()


@app.get("/api/events/health")
async def health():
    return {"status": True}


@app.get("/health")
async def health_alt():
    return {"status": True}


def publish_event(topic: str, event_type: str, data: dict):
    event = {
        "id": str(uuid.uuid4()),
        "type": event_type,
        "data": data,
        "timestamp": datetime.now().isoformat(),
        "source": "events-service"
    }

    producer = get_kafka_producer()
    if producer:
        try:
            future = producer.send(topic, value=event)
            record_metadata = future.get(timeout=10)
            logger.info(
                f"Published event to {topic} at partition {record_metadata.partition} offset {record_metadata.offset}")
            return {
                "status": "success",
                "message": f"Event published to {topic}",
                "event": event,
                "topic": topic,
                "partition": record_metadata.partition,
                "offset": record_metadata.offset
            }
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")
            return {
                "status": "error",
                "message": f"Failed to publish event: {str(e)}",
                "event": event
            }
    else:
        logger.info(f"Event logged (Kafka unavailable): {json.dumps(event, indent=2)}")
        return {
            "status": "success",
            "message": "Event logged (Kafka unavailable)",
            "event": event,
            "warning": "Kafka not available"
        }


@app.post("/api/events/movie")
async def create_movie_event(request: Request):
    try:
        body = await request.json()
        data = body.get('data', body)

        result = publish_event('movie-events', 'movie_event', data)

        if result.get('status') == 'success':
            return JSONResponse(status_code=201, content=result)
        else:
            return JSONResponse(status_code=500, content=result)

    except Exception as e:
        logger.error(f"Error creating movie event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )


@app.post("/api/events/user")
async def create_user_event(request: Request):
    try:
        body = await request.json()
        data = body.get('data', body)

        result = publish_event('user-events', 'user_event', data)

        if result.get('status') == 'success':
            return JSONResponse(status_code=201, content=result)
        else:
            return JSONResponse(status_code=500, content=result)

    except Exception as e:
        logger.error(f"Error creating user event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )


@app.post("/api/events/payment")
async def create_payment_event(request: Request):
    try:
        body = await request.json()
        data = body.get('data', body)

        result = publish_event('payment-events', 'payment_event', data)

        if result.get('status') == 'success':
            return JSONResponse(status_code=201, content=result)
        else:
            return JSONResponse(status_code=500, content=result)

    except Exception as e:
        logger.error(f"Error creating payment event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )


@app.post("/api/events")
async def create_event(request: Request):
    try:
        body = await request.json()
        event_type = body.get('type', 'unknown')
        data = body.get('data', {})

        if 'movie' in event_type.lower():
            topic = 'movie-events'
        elif 'user' in event_type.lower():
            topic = 'user-events'
        elif 'payment' in event_type.lower():
            topic = 'payment-events'
        else:
            topic = 'movie-events'

        result = publish_event(topic, event_type, data)

        if result.get('status') == 'success':
            return JSONResponse(status_code=201, content=result)
        else:
            return JSONResponse(status_code=500, content=result)

    except Exception as e:
        logger.error(f"Error creating event: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Internal server error", "detail": str(e)}
        )


@app.get("/api/events/list")
async def list_events():
    return {
        "status": "success",
        "message": "Events will be displayed here when consumer is running",
        "topics": ["movie-events", "user-events", "payment-events"]
    }


@app.get("/api/events/status")
async def events_status():
    producer_ok = producer is not None
    consumer_ok = consumer is not None

    return {
        "status": "ok",
        "kafka": {
            "brokers": KAFKA_BROKERS,
            "producer": "connected" if producer_ok else "disconnected",
            "consumer": "connected" if consumer_ok else "disconnected"
        },
        "consumer_running": consumer_thread is not None and consumer_thread.is_alive()
    }


@app.get("/")
async def root():
    return {
        "service": "CinemaAbyss Events Service",
        "version": "1.0.0",
        "endpoints": {
            "health": "/api/events/health",
            "movie_event": "POST /api/events/movie",
            "user_event": "POST /api/events/user",
            "payment_event": "POST /api/events/payment",
            "status": "/api/events/status"
        }
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=PORT)
