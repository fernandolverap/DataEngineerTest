import json
import boto3

def lambda_handler(event, context):
    s3 = boto3.resource('s3')
    
    size = s3.Bucket('anoc001-test-data-fernando').Object('raw/data_prueba_tecnica.csv').content_length
    
    metadata = s3.Bucket('anoc001-test-data-fernando').Object('control/metadata.txt').get()['Body'].read()
    
    if int(str(metadata).split('|')[1].replace("'","")) == int (size):
        message = "equal_size"
        s3.Object('anoc001-test-data-fernando','processed/data.csv').copy_from(CopySource='anoc001-test-data-fernando/raw/data_prueba_tecnica.csv')
       s3.Object('anoc001-test-data-fernando','raw/data_prueba_tecnica.csv').delete()
        
    else:
        message = "diferent_size"
    
    

    
    return {
        'statusCode':200,
        'body': message
    }
