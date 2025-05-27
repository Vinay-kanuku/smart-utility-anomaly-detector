# import redis 

# redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)
# redis_client.set('name', 'Vinay')
# value = redis_client.get('name')
# print(value.decode('utf-8'))  # Output: value

from src.utils.stream import harvesine
harvesine(1, 2, 3, 4)





