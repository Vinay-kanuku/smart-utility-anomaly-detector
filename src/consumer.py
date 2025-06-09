import io

import avro.schema
from avro.io import AvroTypeException, BinaryDecoder, DatumReader
from confluent_kafka import Consumer, KafkaError

from constants import SCHEMA_PATH
from stream import RedisStorage, TransactionProcessor


class TransactionConsumer:
    def __init__(self, bootstrap_servers, group_id, topic):
        self.consumer = Consumer(
            {
                "bootstrap.servers": bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            }
        )
        self.consumer.subscribe([topic])
        self.schema = avro.schema.parse(open(SCHEMA_PATH).read())

    def deserialize(self, message):
        """Deserialize Avro binary message to a Python dictionary."""
        if message is None or message.value() is None:
            print("Received empty message")
            return None

        try:
            # Wrap binatttyyyry data in BytesIO

            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            # Wrap binatttyyyry data in BytesIO
            bytes_reader = io.BytesIO(message.value())
            # Create decoder
            decoder = BinaryDecoder(bytes_reader)
            # Create reader with schema
            reader = DatumReader(self.schema)
            # Deserialize to dictionary
            transaction = reader.read(decoder)
            return transaction
        except (AvroTypeException, IOError) as e:
            print(f"Failed to deserialize message: {e}")
            return None

    def consume(self, processor):
        while True:
            msg = self.consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    print("Reached end of partition")
                else:
                    print(f"Error: {msg.error()}")
            else:
                transaction = self.deserialize(msg)
                t = processor.process_transaction(transaction)
                t.update({"is_fraud": transaction.get("is_fraud", False)})
                t.update({"fraud_type": transaction.get("fraud_type", None)})
                print(t)

    def close(self):
        self.consumer.close()


if __name__ == "__main__":
    consumer = TransactionConsumer(
        bootstrap_servers="localhost:29092",
        group_id="transaction_group",
        topic="transactions",
    )
    storage = RedisStorage()
    processor = TransactionProcessor(storage)

    try:
        consumer.consume(processor)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
