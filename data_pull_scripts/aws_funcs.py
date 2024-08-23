import boto3
import json
import pandas as pd
from io import StringIO


def get_ssm_param(param_string, encrypted=False):
    client = boto3.client('ssm', region_name='us-east-1')
    res = client.get_parameter(Name=param_string, WithDecryption=encrypted)
    return res['Parameter']['Value']

def read_data(bucket, key):
    s3 = boto3.client('s3') 
    obj = s3.get_object(Bucket= bucket, Key= key) 
    df = pd.read_csv(obj['Body']) # 'Body' is a key word
    return(df)

def write_data_s3(df, bucket, key):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,index=False)
    s3_resource = boto3.resource('s3')
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())