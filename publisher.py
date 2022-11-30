from google.cloud import pubsub_v1
import google.auth
import json

cred, project = google.auth.default()

with open("test-data.json") as f:
    obj = json.load(f)
    data = json.dumps(obj)

publisher = pubsub_v1.PublisherClient()
topic_path = f"projects/{project}/topics/your-topic-name"

publish_future = publisher.publish(topic_path, data=data.encode("utf-8"))

print("Message Published!")
