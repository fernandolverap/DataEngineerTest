import sys,os
import boto3

paths = []
sizes = []
files = []
input_path_files = sys.argv[1]

print(input_path_files)

for file in os.listdir(input_path_files):
    paths.append(os.path.join(input_path_files,file))
    sizes.append(os.path.getsize(input_path_files+file))
    files.append(file)

metadata=(paths,files,sizes)

print(metadata[0][0],metadata[1][0])





s3 = boto3.client('s3')
s3.upload_file(metadata[0][0],'anoc001-test-data-fernando', 'raw/'+metadata[1][0])

s3.put_object(Body=metadata[1][0]+'|'+str(metadata[2][0]), Bucket='anoc001-test-data-fernando', Key='control/metadata.txt')

