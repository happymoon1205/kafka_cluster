from kafka import KafkaConsumer
import json
import redis

redis_client = redis.Redis(host='34.22.93.125', port=6379, db=0) # 영화 상세페이지를 들어간 로그
redis_client2 = redis.Redis(host='34.22.93.125', port=6379, db=1) # 검색한 검색어의 로그 

# Kafka 처리를 위한 consumer 생성
consumer = KafkaConsumer(
    'user_log3',
    bootstrap_servers=['34.64.190.98:9092', '34.64.190.98:9093', '34.64.190.98:9094'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# 인기영화
def insert_data_redis(log_data):
    
    #상세페이지 본 순위 
    key = 'popularity' # 상세페이지를 많이본 순위를 구현하기위해 
    redis_client.zincrby(key, 1,log_data['action_data']) # sort set의 개념을 활용(순위)
    
    #최근 본 상세페이지 기능 구현
    key2=log_data['user_id'] # 최근 본 영화 구현을 위해
    value=f'{log_data["action_data"]}' # 최근 본 영화 구현을 위해
    redis_client.lpush(key2, value)  # 리스트의 왼쪽에 movie_id 값 추가
    redis_client.ltrim(key2, 0, 9) # 최근 본 영화를 10개만 보여주기위해서 밀어내면서 뒤에오는것은 삭제 
    print(f"레디스에 데이터 추가됨: {key} -> {log_data['action_data']}")

#인기검색어
def insert_data_redis2(log_data):
    #많이 검색한 순위
    key = 'search'
    redis_client.zincrby(key, 1,log_data['action_data']) #movie_id에 검색한 검색어가 들어갈 예정
    print(f"레디스에 데이터 추가됨: {key} -> {log_data['action_data']}")

for msg in consumer:
    log_data = msg.value
    if log_data['info']=='movie_detail':
        insert_data_redis(log_data)
    else:
         insert_data_redis2(log_data)
    