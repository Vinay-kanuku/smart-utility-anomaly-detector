

import redis  
import json 
from utils.stream import harvesine
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import time 
from datetime import datetime, timezone

class RedisStorage:
   def __init__(self, host='localhost', port=6379, db=0):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)
        self.ttl = 3600  # 1 hour in seconds


   def increment_counter(self, key: str, transaction_id, timestamp, window_seconds=60) -> int:
         """
         Increment the counter for a given key in Redis.
         If the key does not exist, it will be initialized to 1.
         """
         # Increment the counter
         try:
            dt = datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
            timestamp = int(dt.timestamp())
         except ValueError:
            timestamp = int(time.time())  # Fallback
    

         self.redis_client.zadd(key, {transaction_id : timestamp}, nx=True)
         self.redis_client.zremrangebyscore(key, '-inf', timestamp -window_seconds)
         count = self.redis_client.zcount(key, timestamp - window_seconds, '+inf')
         self.redis_client.expire(key, window_seconds * 2)
         return count
   

   def add_to_list(self, key: str, value: str, max_len:int=60) -> None:
        """
        Add a value to a Redis list.
        """
        self.redis_client.lpush(key, value)
        self.redis_client.ltrim(key, -max_len, 0)  # Keep only the last max_len elements
        self.redis_client.expire(key, self.ttl)

   def get_list(self, key: str) -> list:
         """
         Get the values from a Redis list.
         """
         return self.redis_client.lrange(key, 0, -1)
   
   def get_value(self, key: str):
         """
         Get a value from Redis.
         """
         val = self.redis_client.get(key)
         return val  
   
         
   def set_value(self, key: str, value: str):
         """
         Set a value in Redis.
         """
         self.redis_client.set(key, value)
         self.redis_client.expire(key, self.ttl)
      
class FeatureExtractor(ABC):
   @abstractmethod
   def extract_features(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        pass
    
class VelocityFeatureExtractor(FeatureExtractor):
   def __init__(self, window_size: int = 1 ):
         self.window_size = window_size

   def extract_features(self, transaction: Dict[str, Any], user_id:str, storage:RedisStorage) -> Dict[str, Any]:

        timestamp = transaction['timestamp']
        transaction_id = transaction['transaction_id']
        window_key = f"user:{user_id}:transactions"
        velocity = storage.increment_counter(window_key, transaction_id, timestamp)
        return {'velocity': velocity}
    
class AmountDeviationFeatureExtractor(FeatureExtractor):
    
   def __init__(self, history_length: int = 60):
        self.history_length = history_length # Number of past transactions to consider for average calculation
   def extract_features(self, transaction: Dict[str, Any], user_id:str, storage:RedisStorage) -> Dict[str, Any]:
        # Example: Calculate amount deviation from average
        # This is a placeholder implementation
        user_id = transaction['user_id']
        amount_key = f"user:{user_id}:amounts"
        amount = transaction['amount']
        storage.add_to_list(amount_key, amount)
        amounts = [float(x) for x in storage.get_list(amount_key)]
        avg_amount = sum(amounts) / len(amounts) if amounts else amount
        amount_deviation = abs(amount - avg_amount)
        
        return {
            'amount_deviation': amount_deviation,
            'average_amount': avg_amount
        }
   
        
class GeoLocationShiftFeatureExtractor(FeatureExtractor):
   def extract_features(self, transaction: Dict[str, Any], user_id:str, storage:RedisStorage) -> Dict[str, Any]:
        # Example: Calculate distance from a known location
        # This is a placeholder implementation

        user_id = transaction['user_id']
        location_key = f"user:{user_id}:locations"
        location = transaction['location']
        last_loc = storage.get_value(location_key)
        if last_loc:
            last_location = json.loads(last_loc)
            distance = harvesine(
                location['lat'], location['lon'],
                last_location['lat'], last_location['lon']
            )
        else:
            distance = 0

        # Optionally, update the last location in storage
        storage.set_value(location_key, json.dumps(location))

        return {
            'geo_location_shift': distance
        }

         
class TransactionProcessor:
   def __init__(self, storage: RedisStorage):
        self.storage = storage
        self.feature_extractors = {
             VelocityFeatureExtractor(),
             AmountDeviationFeatureExtractor(),
             GeoLocationShiftFeatureExtractor()
        }


   def add_feature_extractor(self, feature_name: str, extractor: FeatureExtractor) -> None:
         """
         Add a new feature extractor to the processor.
         """
         self.feature_extractors[feature_name] = extractor

        
   def process_transaction(self, transaction: Dict[str, Any]) -> Dict[str, Any]:
        user_id = transaction['user_id']
        features = {}
        
        for  extractor in self.feature_extractors:
            features.update(extractor.extract_features(transaction, user_id, self.storage))
        
        return features
   

if __name__ == "__main__":
      # Example usage
      storage = RedisStorage()
      processor = TransactionProcessor(storage)
      
      # Simulate a transaction
      transaction = {
         'transaction_id': '12345',
         'user_id': 'user_1',
         'amount': 100.0,
         'timestamp': int(time.time()),
         'location': {'lat': 40.7128, 'lon': -74.0060}
      }
      
      features = processor.process_transaction(transaction)
      print(features)

 

    



