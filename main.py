import google.auth
import apache_beam as beam
import apache_beam.pvalue as pvalue
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json
from typing import List
import json
from copy import deepcopy
import datetime

cred, project = google.auth.default()
dataset, table_name = "your-data-set", "purchase_details"

subs = f"projects/{project}/subscriptions/youe-subscription-name"

table_schema = parse_table_schema_from_json(
    json.dumps(json.load(open("purchase_details.json"))["schema"])
)

def decode_message(message):
    return json.loads(message.data)

class transform_elements(beam.DoFn):
    def process(self, elm):
        bq_elm = {}
        bq_elm["customerName"] = f"{elm['given']} {elm['surname']}"
        bq_elm["itemNo"] = elm["sku"]
        bq_elm["itemPrice"] = elm["price"]
        bq_elm["itemQuantity"] = elm["quantity"]
        bq_elm["purchaseAmount"] = round(elm["quantity"] * elm["price"],2)

        current_time = (datetime.datetime.utcnow().replace(microsecond=0)).strftime("%Y-%m-%d %H:%M:%S.%f UTC")

        bq_elm["insertDateTime"] = current_time

        return [bq_elm]

def dataflow(beam_args: List[str] = None):
    options = PipelineOptions(beam_args, runner="DirectRunner", streaming=True)

    with beam.Pipeline(options=options) as pipe:
        input_data = pipe | "Read from PubSub" >> beam.io.ReadFromPubSub(subscription=subs)

        input_dict = input_data | "Convert to Dict" >> beam.Map(lambda msg: json.loads(msg.decode("utf-8")))

        input_dict | "Input data" >> beam.Map(lambda elm: print(json.dumps(elm)))

        output_dict = input_dict | "Transform" >> beam.ParDo(transform_elements())

        output_dict | "Output data" >> beam.Map(lambda elm: print(json.dumps(elm)))

        output_dict | "Write to BQ" >> beam.io.gcp.bigquery.WriteToBigQuery(table_name, schema=table_schema, dataset=dataset, project=project, insert_retry_strategy="neverRetry", create_disposition="CREATE_NEVER", method="STREAMING_INSERTS")

if __name__ == "__main__":
    dataflow()
