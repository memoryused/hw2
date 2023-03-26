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

list_bucket_contents(bucket='nyc-tlc', match='2017', size_mb=250)

# pip install pyarrow
# Preview Dataset
def preview_dataset(bucket, key):
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

df = preview_dataset(bucket='nyc-tlc', key=f'trip data/yellow_tripdata_2017-01.parquet')
df.head(5)

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

for i in range(1, 6):
    copy_among_buckets(from_bucket='nyc-tlc', from_key=f'trip data/yellow_tripdata_2017-0{i}.parquet',
                      to_bucket='nyc-tlc-cs653-5132', to_key=f'yellow_tripdata-2017-0{i}.parquet')
    
#/////////////////////////////////// Answer a /////////////////////////////////////////
import boto3

s3 = boto3.client('s3')

# Answer a
bucket = 'nyc-tlc-cs653-5132'
key = 'yellow_tripdata-2017-01.parquet'
expression_type = 'SQL'
input_serialization = {'Parquet': {}}
output_serialization = {'CSV': {}}

def creditCard():
    return "Credit card"
def cash():
    return "Cash"
def noCharge():
    return "No charge"
def dispute():
    return "Dispute"
def unknow():
    return "Unknow"
def voidedTrip():
    return "Voided trip"
def default():
    return "Incorrect payment type"

switcher = {
    1: creditCard,
    2: cash,
    3: noCharge,
    4: dispute,
    5: unknow,
    6: voidedTrip
    }

def switch(payment_type):
    return switcher.get(payment_type, default)()

sum = 0
for i in range(1, 6):
    # Execute S3 Select query
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=f"select count(payment_type) from s3object s where payment_type = {i}",
        ExpressionType=expression_type,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )
    
    # Iterate through the response and print each line
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            sum = sum + int(records)
            print(f"ประเภทการจ่ายเงิน {switch(i)} มีจำนวน yellow taxi rides เท่ากับ {records}")
print(f"มีจำนวน yellow taxi rides ทั้งหมดเท่ากับ {sum}") 

#/////////////////////////////////// Answer b /////////////////////////////////////////
import numpy as np
PUL_ID = df['PULocationID'].unique()
np.sort(PUL_ID)

import boto3

s3 = boto3.client('s3')

# Answer a
bucket = 'nyc-tlc-cs653-5132'
key = 'yellow_tripdata-2017-01.parquet'
expression_type = 'SQL'
input_serialization = {'Parquet': {}}
output_serialization = {'CSV': {}}

# คำนวนค่าโดยสารรวมของ rides ในแต่ละจุด
def sum_total_amount(pulid):
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=f"select sum(total_amount)  from s3object s where PULocationID = {i}",
        ExpressionType=expression_type,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )
    
    # Iterate through the response and print each line
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            
            try:
                isinstance(float(records), float)
                return float(records)
            except:
                return None

# จำนวนผู้โดยสารเฉลี่ยต่อ rides ในแต่ละจุด
def avg_passenger_count(pulid):
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=f"select avg(passenger_count) from s3object s where PULocationID = {i}",
        ExpressionType=expression_type,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )
    
    # Iterate through the response and print each line
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            
            try:
                isinstance(float(records), float)
                return float(records)
            except:
                return None            

# Define 3 array, keep to show in DataFrame
pul_id_arr=[]
total_fare_arr=[]
avg_pass_arr=[]

for i in range(1, 266):
    # Execute S3 Select query
    response = s3.select_object_content(
        Bucket=bucket,
        Key=key,
        Expression=f"select count(PULocationID) from s3object s where PULocationID = {i}",
        ExpressionType=expression_type,
        InputSerialization=input_serialization,
        OutputSerialization=output_serialization,
    )
    
    # Iterate through the response and print each line
    for event in response['Payload']:
        if 'Records' in event:
            records = event['Records']['Payload'].decode('utf-8')
            
            pul_id_arr.append(i)
            print(f"จุดรับผู้โดยสารที่ {i} มีจำนวน Yellow taxi rides เท่ากับ {int(records)}")
            
            total_fare = sum_total_amount(i)
            if isinstance(total_fare, float):
                total_fare = float("{:.2f}".format(total_fare))
                total_fare_arr.append(total_fare)
                print(f"ค่าโดยสารรวม {total_fare} $")
            else:
                total_fare = 0.0
                total_fare_arr.append(total_fare)
                print(f"ค่าโดยสารรวม - $")
                
            avg_pass = avg_passenger_count(i)
            if isinstance(avg_pass, float):
                avg_pass = float("{:.2f}".format(avg_pass))
                avg_pass_arr.append(avg_pass)
                print(f"จำนวนผู้โดยสารเฉลี่ยต่อ rides เท่ากับ {avg_pass} คน")
            else:
                avg_pass = 0.0
                avg_pass_arr.append(avg_pass)
                print(f"จำนวนผู้โดยสารเฉลี่ยต่อ rides เท่ากับ  - คน")
            print(f"-----------------------------------------")

label_data = {'จุดรับ': pul_id_arr, 
              'ค่าโดยสารรวม': total_fare_arr, 
              'จำนวนผู้โดยสารเฉลี่ยต่อ rides': avg_pass_arr}
dfm = pd.DataFrame(label_data)
dfm

#/////////////////////////////////// Answer c /////////////////////////////////////////
import boto3

s3 = boto3.client('s3')

# Define parameter
bucket = 'nyc-tlc-cs653-5132'
expression_type = 'SQL'
input_serialization = {'Parquet': {}}
output_serialization = {'CSV': {}}

# Define 6 array of payment type, keep to show in DataFrame
cc_arr = []
c_arr = []
nc_arr = []
dp_arr = []
uk_arr = []
vt_arr = []
summary = []

def creditCard():
    cc_arr.append(records)
    return "Credit card"
def cash():
    c_arr.append(records)
    return "Cash"
def noCharge():
    nc_arr.append(records)
    return "No charge"
def dispute():
    dp_arr.append(records)
    return "Dispute"
def unknow():
    uk_arr.append(records)
    return "Unknow"
def voidedTrip():
    vt_arr.append(records)
    return "Voided trip"
def default():
    print("Incorrect payment type")

switcher = {
    1: creditCard,
    2: cash,
    3: noCharge,
    4: dispute,
    5: unknow,
    6: voidedTrip
    }

def switch(payment_type):
    return switcher.get(payment_type, default)()

def sum_rides_by_month(month):
    sum = 0
    for i in range(1, 6):
        # Execute S3 Select query for answer a
        response = s3.select_object_content(
            Bucket=bucket,
            Key=f'yellow_tripdata-2017-0{month}.parquet',
            Expression=f"select count(payment_type) from s3object s where payment_type = {i}",
            ExpressionType=expression_type,
            InputSerialization=input_serialization,
            OutputSerialization=output_serialization,
        )
    
        # Iterate through the response and print each line
        for event in response['Payload']:
            if 'Records' in event:
                records = event['Records']['Payload'].decode('utf-8')

                try:
                    isinstance(int(records), int)
                    records = int(records)
                except:
                    return None 

                sum = sum + records
                print(f"ในเดือน {month} ที่ประเภทการจ่ายเงิน {switch(i)} มีจำนวน yellow taxi rides เท่ากับ {records}")

    summary.append(sum)
    print(f"เดือน {month} มีจำนวน yellow taxi rides รวมทั้งสิ้น {sum} รอบ")
    print(f"-----------------------------------------")

    