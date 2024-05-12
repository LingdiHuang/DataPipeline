from datetime import date, timedelta
import requests
from google.cloud import storage, bigquery
import csv
from constant import CREATE_COVID_TABLES, TABLE_ID, MERGE_COVID_TABLES,  base_url, DATA_COLUMNS, bigquery_client, bucket, storage_client
import pandas as pd
from io import StringIO

# Define the start and end dates for your data retrieval

def fetch_raw_data (start_date, end_date, **kwargs):
    
    current_date = start_date

    while current_date <= end_date:
        file_date = current_date.strftime("%m-%d-%Y")
        file_url = base_url + f"{file_date}.csv"
        response = requests.get(file_url)

        if response.status_code == 200:

            response_obj = response.json()['payload']['blob']['csv']
            df = pd.DataFrame(response_obj)
            new_header = df.iloc[0]
            df = df[1:]
            df.columns = new_header

            for col in DATA_COLUMNS:
                if col not in df.columns:
                    df[col] = None

            buffer = StringIO()
            df.to_csv(buffer, index=False, columns=DATA_COLUMNS,quoting=csv.QUOTE_ALL)
            body = buffer.getvalue()

            blob = bucket.blob(f"covid_data_update/{file_date}.csv")
            
            blob.upload_from_string(body, content_type='text/csv')

            print(f"Uploaded {file_date}.csv to Google Cloud Storage")
        else:
            print(f"Failed to download {file_date}.csv")

        current_date += timedelta(days=1)

    return


def load_data (**kwargs):

    #Create data schema & table
    query_job = bigquery_client.query(CREATE_COVID_TABLES)
    query_job.result()

    print("Created routine {}".format(query_job.ddl_target_routine))

    #Load Data
    schema_list = []
    get_schema = bigquery_client.get_table(TABLE_ID)
    for field in get_schema.schema:
        schema_list.append(bigquery.SchemaField(name=field.name, field_type=field.field_type, mode =field.mode))

    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema_list
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.field_delimiter = ','
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.null_marker = ''
    job_config.skip_leading_rows=1

    bucket_name = 'final_proejct'
    prefix = 'covid_data_update/'


    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)

    for blob in blobs:
        uri = f'gs://final_proejct/{blob.name}'
        job = bigquery_client.load_table_from_uri(uri, TABLE_ID, job_config=job_config)
        job.result()  # Wait for the job to complete

        destination_table = bigquery_client.get_table(TABLE_ID)
        print(f'Loaded {destination_table.num_rows} rows from {blob}.')
    
    return