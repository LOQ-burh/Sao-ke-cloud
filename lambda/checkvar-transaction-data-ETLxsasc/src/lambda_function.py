import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus


from processors.file_processor import FileProcessorFactory, FileProcessor
from utils.mongo_utils import import_json_to_mongodb

print('Loading function')
s3_client = boto3.client('s3')

def lambda_handler(event, context):
    print("eventRecords", event['Records'][0]['s3'])
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        download_path = '/tmp/' + key.replace('/', '')
        s3_client.download_file(bucket, key, download_path)

        # Determine file type and process
        file_extension = os.path.splitext(key)[1].lower()
        
        file_size = os.path.getsize(download_path)
        print(f'Dung lượng file download từ S3: {file_size} bytes')
        print('file_extension', file_extension)

        processor = FileProcessorFactory.get_processor(file_extension)
        processed_file = processor.process(download_path)
        print('processed_file', processed_file)

        # Further operations, like MongoDB import
        if file_extension == '.pdf':
            mongo_uri = os.environ['MONGO_URI']
            import_json_to_mongodb(processed_file, "checkvar", "trans", mongo_uri)