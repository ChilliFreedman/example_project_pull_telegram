from kafka import KafkaConsumer
import json
import os

class SetConsumer:

    def __init__(self):
        self.consumer = None


    def get_consumer_events(self,topic):
        try:

            kafka_url = os.getenv('KAFKA_URL', 'localhost')
            kafka_port = os.getenv('KAFKA_PORT', '9092')
            bootstrap_servers = [f'{kafka_url}:{kafka_port}']
            consumer = KafkaConsumer(topic,
                                     group_id='my-group',
                                     value_deserializer=lambda m: json.loads(m.decode('ascii')),
                                     bootstrap_servers=bootstrap_servers)  # ,consumer_timeout_ms=10000)
            self.consumer = consumer
            print("In file 'kafka_consumer' the config was successful")
        except Exception as e:
            print(f"In file 'kafka_consumer' an unexpected error occurred: {e}")

    def print_messages(self):
        for message in self.consumer:
            print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                 message.offset, message.key,
                                                 message.value))
#test to see how the data returns from topic 'message_id': 465, 'group_link': 'https://t.me/+ztOtIXepdDVkYjE0', 'file_id': '68c908ff6534e228cd6aaf79', 'category': 'audio'
    def get_messages_in_list(self):

        for message in self.consumer:

                print(f"'message_id': {message.value['message_id']}")
                print(f"'group_link': {message.value['group_link']}")
                print(f"'category': {message.value['category']}")
                print(f"'file_id': {message.value['file_id']}")

                # file_name = message.value['file_id']
                # print(file_name)
                # print((hash(file_name)))


if __name__ == "__main__":
    set_consumer = SetConsumer()
    set_consumer.get_consumer_events("telegram_messages")
    #set_consumer.print_messages()
    set_consumer.get_messages_in_list()
    #set_consumer.get_messages_in_list()
