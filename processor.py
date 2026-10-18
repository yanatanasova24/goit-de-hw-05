
import json
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP_SERVERS = "localhost:9092"

SENSORS_TOPIC = "building_sensors_yana"
TEMPERATURE_TOPIC = "temperature_alerts_yana"
HUMIDITY_TOPIC = "humidity_alerts_yana"

consumer = KafkaConsumer(
    SENSORS_TOPIC,
    bootstrap_servers=BOOTSTRAP_SERVERS,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="sensor_processor_group",
    value_deserializer=lambda value: json.loads(
        value.decode("utf-8")
    ),
)

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(
        value, ensure_ascii=False
    ).encode("utf-8"),
)

print("Обробник запущено. Очікування даних...")

try:
    for message in consumer:
        data = message.value

        sensor_id = data["sensor_id"]
        timestamp = data["timestamp"]
        temperature = data["temperature"]
        humidity = data["humidity"]

        print(
            f"Датчик {sensor_id}: "
            f"температура={temperature}°C, "
            f"вологість={humidity}%"
        )

        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "message": (
                    f"УВАГА! Температура {temperature}°C "
                    "перевищує допустимий поріг 40°C."
                ),
            }

            producer.send(
                TEMPERATURE_TOPIC, value=alert).get(timeout=10)

            print(f"[ВІДПРАВЛЕНО] Топік: {TEMPERATURE_TOPIC}")
            print(json.dumps(alert, ensure_ascii=False))

        if humidity > 80 or humidity < 20:
            if humidity > 80:
                description = (
                    f"Вологість {humidity}% перевищує "
                    "допустимий поріг 80%."
                )
            else:
                description = (
                    f"Вологість {humidity}% нижча "
                    "за допустимий поріг 20%."
                )

            alert = {
                "sensor_id": sensor_id,
                "timestamp": timestamp,
                "temperature": temperature,
                "humidity": humidity,
                "message": f"УВАГА! {description}",
            }

            producer.send(
                HUMIDITY_TOPIC, value=alert
            ).get(timeout=10)

            print(f"[ВІДПРАВЛЕНО] Топік: {HUMIDITY_TOPIC}")
            print(json.dumps(alert, ensure_ascii=False))

except KeyboardInterrupt:
    print("\nОбробник зупинено.")

finally:
    consumer.close()
    producer.flush()
    producer.close()