import functions_framework
import re

# Libraries
from google.cloud import bigquery

# Construct a BigQuery client object.
client = bigquery.Client()

stage_table_id = "[gcp-project].[bq-dataset].employee_stage"
table_id = "[gcp-project].[bq-dataset].employee"

file_pattern = r".*sample_[0-9]{8}\.csv$"

table_schema = [
    bigquery.SchemaField("id", "STRING"),
    bigquery.SchemaField("name", "STRING"),
    bigquery.SchemaField("birth_date", "STRING"),
    bigquery.SchemaField("occupation", "STRING"),
    bigquery.SchemaField("gender", "STRING"),
]

# Triggered when file is created in storage bucket
@functions_framework.cloud_event
def main(cloud_event):
    data = cloud_event.data

    bucket = data["bucket"]
    name = data["name"]

    print(f"Bucket: {bucket}")
    print(f"File: {name}")

    if not re.match(file_pattern, name):
        print(f'{name} not match file_pattern')
        return

    job_config = bigquery.LoadJobConfig(
        schema = table_schema,
        skip_leading_rows = 1,
        # The source format defaults to CSV, so the line below is optional.
        source_format = bigquery.SourceFormat.CSV,
        write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE,
    )

    gcs_uri = f"gs://{bucket}/{name}"

    load_job = client.load_table_from_uri(
        gcs_uri, stage_table_id, job_config=job_config
    ) # Make an API request.

    load_job.result()  # Waits for the job to complete.

    destination_table = client.get_table(stage_table_id)  # Make an API request.
    print("Loaded {} rows.".format(destination_table.num_rows))

    # Merge Query
    merge_sql_file = open('merge.sql','r')
    merge_sql = merge_sql_file.read()
    merge_sql = merge_sql.replace('$stage_table_id', stage_table_id)
    merge_sql = merge_sql.replace('$table_id', table_id)

    query_job = client.query(
        merge_sql,
        # Explicitly force job execution to be routed to a specific processing
        # location.
        location="US",
        # The client libraries automatically generate a job ID. Override the
        # generated ID with either the job_id_prefix or job_id parameters.
        job_id_prefix="employee_merge_",
    )  # Make an API request.

    query_job.result()
    print("Completed job: {}".format(query_job.job_id))
