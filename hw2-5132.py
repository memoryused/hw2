import boto3
import botocore
import pandas as pd
from IPython.display import display, Markdown

s3 = boto3.client('s3')
s3_resource = boto3.resource('s3')

# Create Bucket
def create_bucket(bucket):
    import logging

    try:
        s3.create_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        logging.error(e)
        return 'Bucket ' + bucket + ' could not be created.'
    return 'Created or already exists ' + bucket + ' bucket.'

create_bucket('nyc-tlc-cs653-5132')

# List Buckets
def list_buckets(match=''):
    response = s3.list_buckets()
    if match:
        print(f'Existing buckets containing "{match}" string:')
    else:
        print('All existing buckets:')
    for bucket in response['Buckets']:
        if match:
            if match in bucket["Name"]:
                print(f'  {bucket["Name"]}')

list_buckets(match='nyc')

# List Bucket Contents
def list_bucket_contents(bucket, match='', size_mb=0):
    bucket_resource = s3_resource.Bucket(bucket)
    total_size_gb = 0
    total_files = 0
    match_size_gb = 0
    match_files = 0
    for key in bucket_resource.objects.all():
        key_size_mb = key.size/1024/1024
        total_size_gb += key_size_mb
        total_files += 1
        list_check = False
        if not match:
            list_check = True
        elif match in key.key:
            list_check = True
        if list_check and not size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')
        elif list_check and key_size_mb <= size_mb:
            match_files += 1
            match_size_gb += key_size_mb
            print(f'{key.key} ({key_size_mb:3.0f}MB)')

    if match:
        print(f'Matched file size is {match_size_gb/1024:3.1f}GB with {match_files} files')            
    
    print(f'Bucket {bucket} total size is {total_size_gb/1024:3.1f}GB with {total_files} files')

print('############### list_bucket_contents ')
list_bucket_contents(bucket='nyc-tlc', match='2017', size_mb=250)

# Preview CSV Dataset
def preview_csv_dataset(bucket, key, rows=10):
    data_source = {
            'Bucket': bucket,
            'Key': key
        }
    # Generate the URL to get Key from Bucket
    url = s3.generate_presigned_url(
        ClientMethod = 'get_object',
        Params = data_source
    )

    # data = pd.read_csv(url, nrows=rows)
    data = pd.read_parquet(url, engine='pyarrow')
    return data

# Preview CSV Dataset
print('############### preview dataset')
df = preview_csv_dataset(bucket='nyc-tlc', key='trip data/green_tripdata_2017-01.parquet', rows=100)
df.head()
df.shape
df.info()
df.describe()

# Copy Among Buckets
def key_exists(bucket, key):
    try:
        s3_resource.Object(bucket, key).load()
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == "404":
            # The key does not exist.
            return(False)
        else:
            # Something else has gone wrong.
            raise
    else:
        # The key does exist.
        return(True)

def copy_among_buckets(from_bucket, from_key, to_bucket, to_key):
    if not key_exists(to_bucket, to_key):
        s3_resource.meta.client.copy({'Bucket': from_bucket, 'Key': from_key}, 
                                        to_bucket, to_key)        
        print(f'File {to_key} saved to S3 bucket {to_bucket}')
    else:
        print(f'File {to_key} already exists in S3 bucket {to_bucket}') 

print('############### Copy Among Buckets')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-01.parquet',
                      to_bucket='nyc-tlc-cs653-5132', to_key='few-trips/trips-2017-01.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-02.parquet',
                      to_bucket='nyc-tlc-cs653-5132', to_key='few-trips/trips-2017-02.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/yellow_tripdata_2017-03.parquet',
                      to_bucket='nyc-tlc-cs653-5132', to_key='few-trips/trips-2017-03.parquet')
copy_among_buckets(from_bucket='nyc-tlc', from_key='trip data/green_tripdata_2017-01.parquet',
                      to_bucket='nyc-tlc-cs653-5132', to_key='few-trips/green_tripdata_2017-01.parquet')

# Set up S3 Select parameters
query1 ="select payment_type, count(payment_type) from s3object s group by s.payment_type"
query2 ="select PULocationID, count(PULocationID) as rides, sum(total_amount) as 'Total amount', avg(passenger_count) as 'AVG passenger count' from s3object s group by s.PULocationID"
query3 =""
bucket = 'nyc-tlc-cs653-5132'
key = 'few-trips/trips-2017-01.parquet'
expression_type = 'SQL'
input_serialization = {'Parquet': {}}
output_serialization = {'CSV': {}}

# Execute S3 Select query
def s3_select(bucket, key, query):
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=query,
        ExpressionType=expression_type,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )

    # Iterate through the response and print each line
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            print(records)

s3_select(bucket, key, query1)
s3_select(bucket, key, query2)
s3_select(bucket, key, query3)