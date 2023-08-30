from kafka import KafkaConsumer
import json
from google.cloud import bigquery
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file('/Users/gangsickmun/kafkaDocker/finalproject1-393400-9fef84669d36.json')
client = bigquery.Client(credentials=credentials)
table_id = "finalproject1-393400.user_log.kafkabig"

# Kafka 처리를 위한 consumer 생성
consumer = KafkaConsumer(
    'user_log2',
    bootstrap_servers=['34.64.190.98:9092', '34.64.190.98:9093', '34.64.190.98:9094'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)
# 로그 데이터를 처리하여 BigQuery에 저장하는 함수
def insert_data_bigquery(log_data):
    errors = client.insert_rows_json(table_id, [log_data])
    if errors == []:
        print("새 행이 추가되었습니다.")
    else:
        print("다음 에러가 발생했습니다: ", errors)

for msg in consumer:
    log_data = msg.value
    insert_data_bigquery(log_data)
