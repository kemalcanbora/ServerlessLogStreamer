from redisearch import Client, Query, TextField, NumericField
from redis.client import ResponseError
from uuid import uuid4
import json
import os
import ast

class Redis:
    def __init__(self):
        self.SCHEMA = (
            NumericField("timestamp"),
            TextField("message"),
            NumericField("ingestionTime")
        )

    def connection(self):
        client = Client(index_name=os.environ["redis_index_name"],
                        host=os.environ["redis_host"],
                        port=ast.literal_eval(os.environ["redis_port"]),
                        password=os.environ["redis_password"])
        self.get_or_create_index(client)
        return client

    def get_or_create_index(self, client: Client):
        try:
            return client.info()
        except ResponseError:
            return client.create_index(self.SCHEMA)

    def set_value(self, client: Client, doc_id: str, doc: str):
        client.redis.hmset(doc_id, mapping=doc)

    def search_value(self, client: Client, search_text: str):
        q = Query(search_text).with_scores().paging(0, 5)
        return client.search(q)


class SubPub(Redis):
    def __init__(self):
        super(SubPub, self).__init__()
        self.client = Redis().connection()

    def publisher(self, channel, message):
        Redis().set_value(client=self.client,
                          doc_id=channel + ":" + uuid4().__str__(),
                          doc=message)

        self.client.redis.publish(channel=channel, message=json.dumps(message))

    def subscriber(self, channels):
        r = self.client.redis.pubsub()
        r.subscribe(channels)
        while True:
            message = r.get_message()
            if message:
                print(message)