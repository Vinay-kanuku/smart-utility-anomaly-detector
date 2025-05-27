from confluent_kafka import Producer
from faker import Faker
import avro.schema
from avro.io import DatumWriter, BinaryEncoder
import io
import random
from datetime import datetime, timedelta, timezone
import time
from typing import Dict, Any, Optional
from constants import SCHEMA_PATH


class TransactionGenerator:
    """Handles generation of transaction data with fraud patterns."""
    
    def __init__(self):
        self.fake = Faker()
        self.fraud_types = [
              'velocity', 'unusual_amount', 'geo_velocity',
            # 'device_anomaly', 'multiple_card_usage', 'merchant_anomaly',
            # 'time_anomaly',  'declined_pattern'
        ]
        self.risky_countries = ['Russia', 'Nigeria', 'China']
        self.suspicious_merchants = ['RiskyRetailer Inc.', 'ShadyShop LLC', 'FraudMart']
    
    def generate_base_transaction(self) -> Dict[str, Any]:
        """Generate a base transaction with common fields."""
        return {
            'transaction_id': self.fake.uuid4(),
            'user_id': self.fake.uuid4(),
            'amount': round(random.uniform(5.0, 1000.0), 2),
            'timestamp': datetime.now(timezone.utc).isoformat(),
            'merchant': self.fake.company(),
            'card_number': self.fake.credit_card_number(),
            'location': {
                'lat': float(self.fake.latitude()),
                'lon': float(self.fake.longitude()),
                'country': self.fake.country()
            },
            'device_id': self.fake.uuid4(),
            'is_fraud': False,
            'fraud_type': None,
            'status': 'approved'
        }
    
    def apply_fraud_pattern(self, transaction: Dict[str, Any], fraud_type: str) -> None:
        """Apply specific fraud pattern to transaction."""
        transaction['is_fraud'] = True
        transaction['fraud_type'] = fraud_type
        
        fraud_patterns = {
            'card_not_present': self._apply_card_not_present,
            'location_mismatch': self._apply_location_mismatch,
            'velocity': self._apply_velocity,
            'unusual_amount': self._apply_unusual_amount,
            'device_anomaly': self._apply_device_anomaly,
            'multiple_card_usage': self._apply_multiple_card_usage,
            'merchant_anomaly': self._apply_merchant_anomaly,
            'time_anomaly': self._apply_time_anomaly,
            'geo_velocity': self._apply_geo_velocity,
            'declined_pattern': self._apply_declined_pattern
        }
        
        if fraud_type in fraud_patterns:
            fraud_patterns[fraud_type](transaction)
    
    def _apply_card_not_present(self, transaction: Dict[str, Any]) -> None:
        transaction['merchant'] = f"Online-{self.fake.company()}"
        transaction['amount'] = round(random.uniform(200.0, 5000.0), 2)
    
    def _apply_location_mismatch(self, transaction: Dict[str, Any]) -> None:
        transaction['location']['country'] = random.choice(self.risky_countries)
    
    def _apply_velocity(self, transaction: Dict[str, Any]) -> None:
        transaction['timestamp'] = (
            datetime.now(timezone.utc) - 
            timedelta(seconds=random.randint(1, 600))
        ).isoformat()
    
    def _apply_unusual_amount(self, transaction: Dict[str, Any]) -> None:
        transaction['amount'] = round(random.uniform(1000.0, 10000.0), 2)
    
    def _apply_device_anomaly(self, transaction: Dict[str, Any]) -> None:
        transaction['device_id'] = f"suspicious-{self.fake.uuid4()}"
    
    def _apply_multiple_card_usage(self, transaction: Dict[str, Any]) -> None:
        transaction['user_id'] = "fixed-user-uuid"
        transaction['card_number'] = self.fake.credit_card_number()
    
    def _apply_merchant_anomaly(self, transaction: Dict[str, Any]) -> None:
        transaction['merchant'] = random.choice(self.suspicious_merchants)
    
    def _apply_time_anomaly(self, transaction: Dict[str, Any]) -> None:
        transaction['timestamp'] = datetime.now(timezone.utc).replace(
            hour=3, minute=0
        ).isoformat()
    
    def _apply_geo_velocity(self, transaction: Dict[str, Any]) -> None:
        transaction['location'] = {
            'lat': 0, 
            'lon': 0, 
            'country': 'Australia'
        }
    
    def _apply_declined_pattern(self, transaction: Dict[str, Any]) -> None:
        transaction['status'] = 'declined'
    
    def generate_transaction(self, is_fraud: bool = False) -> Dict[str, Any]:
        """Generate a complete transaction with optional fraud patterns."""
        transaction = self.generate_base_transaction()
        
        if is_fraud:
            fraud_type = random.choice(self.fraud_types)
            self.apply_fraud_pattern(transaction, fraud_type)
        
        return transaction


class AvroSerializer:
    """Handles Avro serialization of transactions."""
    
    def __init__(self, schema_path: str):
        self.schema = avro.schema.parse(open(schema_path).read())
    
    def serialize(self, transaction: Dict[str, Any]) -> bytes:
        """Serialize transaction to Avro binary format."""
        writer = DatumWriter(self.schema)
        bytes_writer = io.BytesIO()
        encoder = BinaryEncoder(bytes_writer)
        writer.write(transaction, encoder)
        return bytes_writer.getvalue()


class KafkaMessageHandler:
    """Handles Kafka message delivery callbacks."""
    
    @staticmethod
    def delivery_report(err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            print(f'Message delivery failed: {err}')
        else:
            print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


class TransactionProducer:
    """Main class for producing transactions to Kafka."""
    
    def __init__(self, kafka_config: Dict[str, Any], schema_path: str = SCHEMA_PATH):
        self.producer = Producer(kafka_config)
        self.transaction_generator = TransactionGenerator()
        self.serializer = AvroSerializer(schema_path)
        self.message_handler = KafkaMessageHandler()
    
    def produce_single_transaction(self, topic: str, is_fraud: bool = False) -> None:
        """Produce a single transaction to Kafka."""
        transaction = self.transaction_generator.generate_transaction(is_fraud)
        serialized_data = self.serializer.serialize(transaction)
        
        self.producer.produce(
            topic, 
            value=serialized_data, 
            callback=self.message_handler.delivery_report
        )
        self.producer.poll(0)
    
    def produce_transactions(self, topic: str = 'transactions', 
                           num_transactions: int = 100, 
                           fraud_ratio: float = 0.6,
                           delay_seconds: float = 0.01) -> None:
        """Produce multiple transactions to Kafka with specified fraud ratio."""
        for i in range(num_transactions):
            is_fraud = random.random() < fraud_ratio
            self.produce_single_transaction(topic, is_fraud)
            
            if delay_seconds > 0:
                time.sleep(delay_seconds)
            
            # if (i + 1) % 10 == 0:
            #     print(f"Produced {i + 1}/{num_transactions} transactions")
        
        self.producer.flush()
        # print(f"Successfully produced {num_transactions} transactions to topic '{topic}'")
    
    def close(self) -> None:
        """Clean up producer resources."""
        self.producer.flush()


class TransactionProducerFactory:
    """Factory class for creating TransactionProducer instances."""
    
    @staticmethod
    def create_producer(bootstrap_servers: str = 'localhost:29092',
                       additional_config: Optional[Dict[str, Any]] = None,
                       schema_path: str = SCHEMA_PATH) -> TransactionProducer:
        """Create a TransactionProducer with default or custom configuration."""
        kafka_config = {'bootstrap.servers': bootstrap_servers}
        
        if additional_config:
            kafka_config.update(additional_config)
        
        return TransactionProducer(kafka_config, schema_path)


# Usage example
if __name__ == '__main__':
    # Create producer using factory
    producer = TransactionProducerFactory.create_producer()
    
    try:
        # Produce transactions
        producer.produce_transactions(
            topic='transactions',
            num_transactions=100,
            fraud_ratio=0.3,
            delay_seconds=0.1
        )
        producer.close()
        exit(0)
    finally:
        # Clean up
        producer.close()