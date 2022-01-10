import json
import boto3
import pandas as pd 
import io 
import os

s3 = boto3.resource('s3')



def lambda_handler(event, context):
    bucket = s3.Bucket('anoc001-test-data-fernando')
    prefix = 'processed/'
    
    for object_summary in bucket.objects.filter(Prefix=prefix):
        key = object_summary.key
        if key.endswith("csv"):
            print(key)
            file_name = key.split('/',1)
            
            response = object_summary.get()
            data = pd.read_csv(response['Body'],sep=',')
            if data.isnull().values.any():
                message = 'datawithnulls'
            else:
                message = 'datacorrect'

            
            data['created_at'] = pd.to_datetime(data['created_at']).dt.date
            data['name'].replace('P.*','MiPasajefy',regex=True, inplace = True)
            agregate = data.groupby(['name','created_at']).sum('amount')
            
            
            
            with io.StringIO() as csv_buffer:
                agregate.to_csv(csv_buffer)
                ##print(csv_buffer.getvalue())
                s3.Object('anoc001-test-data-fernando','final/'+str(file_name[1])).put(Body=csv_buffer.getvalue())
            
    return {
        'statusCode': 200,
        'body': message
    }
