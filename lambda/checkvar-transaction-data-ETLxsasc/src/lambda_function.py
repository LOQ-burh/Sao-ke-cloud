# Phong Bat Detector 2

import boto3
import os
import sys
import uuid
from urllib.parse import unquote_plus
#from PIL import Image
#import PIL.Image

import os
import pdfplumber
import re
import PyPDF2
import json

from pymongo import MongoClient

print('Loading function')

s3_client = boto3.client('s3')

def split_pdf(pdf_path, output_dir, num_pages_per_file=100):
  """
  Hàm tách PDF thành nhiều file nhỏ hơn.

  Args:
      pdf_path: Đường dẫn đến tệp PDF.
      output_dir: Thư mục để lưu các file PDF nhỏ.
      num_pages_per_file: Số trang mỗi file PDF nhỏ (tùy chọn).
  """
  if not os.path.exists(output_dir):
        os.makedirs(output_dir)

  with open(pdf_path, 'rb') as pdf_file:
    pdf_reader = PyPDF2.PdfReader(pdf_file)
    num_pages = len(pdf_reader.pages)

    for i in range(0, num_pages, num_pages_per_file):
      pdf_writer = PyPDF2.PdfWriter()
      for page_num in range(i, min(i + num_pages_per_file, num_pages)):
        pdf_writer.add_page(pdf_reader.pages[page_num])
      output_filename = f"{output_dir}/part_{i // num_pages_per_file + 1}.pdf"
      with open(output_filename, 'wb') as output_file:
        pdf_writer.write(output_file)


def merge_json_files(json_files, output_file):
  """
  Hàm kết hợp các file JSON thành một file JSON lớn.

  Args:
      json_files: Danh sách các file JSON cần kết hợp.
      output_file: Tên file JSON để lưu kết quả.
  """
  all_data = []
  for json_file in json_files:
    with open(json_file, "r") as f:
      data = json.load(f)
      all_data.extend(data)

  # Lưu JSON vào file
  with open(output_file, "w") as f:
    json.dump(all_data, f, indent=4)


def extract_tables_to_json(pdf_path, output_file="table_data.json"):
  """
  Hàm trích xuất dữ liệu từ tất cả bảng trong PDF và lưu vào file JSON.

  Args:
      pdf_path: Đường dẫn đến tệp PDF.
      output_file: Tên file JSON để lưu kết quả (tùy chọn).
  """
  json_data = []
  with pdfplumber.open(pdf_path) as pdf:
    for page_num in range(len(pdf.pages)):
      page = pdf.pages[page_num]
      tables = page.extract_tables()
      for table_data in tables:
        # Bỏ qua header (2 dòng đầu tiên)
        for row in table_data[1:]:
          # Kiểm tra nếu dòng có dữ liệu (không rỗng)
          if any(cell.strip() for cell in row):
            json_data.append({
              "date": row[1].strip(),
              "amount": row[4].strip().replace(".","").replace(",00","").replace(" ",""),
              "notes": row[5].strip(),
              "code": None,
              "bankAccountNumber": row[2].strip(),
              "source": "bidv-042209"
            })

  # Lưu JSON vào file
  with open(output_file, "w") as f:
    json.dump(json_data, f, indent=4)
    # json.dump(split_transactions(json_data), f, indent=4)


def import_json_to_mongodb(json_file, db_name, collection_name, mongo_uri="mongodb://localhost:27017/"):
    """
    Hàm để import dữ liệu từ file JSON vào MongoDB.

    Args:
        json_file: Đường dẫn đến file JSON cần import.
        db_name: Tên database trên MongoDB.
        collection_name: Tên collection trên MongoDB.
        mongo_uri: URI kết nối MongoDB (mặc định là localhost).
    """
    # Kết nối tới MongoDB
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]

    # Đọc file JSON
    with open(json_file, "r") as f:
        data = json.load(f)

    # Import dữ liệu vào collection
    if isinstance(data, list):
        # Nếu file JSON chứa một danh sách các documents
        collection.insert_many(data)
    else:
        # Nếu file JSON chỉ chứa một document
        collection.insert_one(data)

    print(f"Imported {len(data)} records into {db_name}.{collection_name}")



def lambda_handler(event, context):
    print("eventRecords", event['Records'][0]['s3'])
    
    for record in event['Records']:
        bucket = record['s3']['bucket']['name']
        key = unquote_plus(record['s3']['object']['key'])
        print("key", key)
        tmpkey = key.replace('/', '')
        print("tmpkey", tmpkey)
        #download_path = '/tmp/{}{}'.format(uuid.uuid4(), tmpkey)
        download_path = '/tmp/{}'.format(tmpkey)
        upload_path = '/tmp/resized-{}'.format(tmpkey)
        s3_client.download_file(bucket, key, download_path)
        print("download_path", download_path)
        print("upload_path", upload_path)
        print("record", record)
        file_size = os.path.getsize(download_path)
        print(f'Dung lượng file download từ S3: {file_size} bytes')

        # resize_image(download_path, upload_path)
        # s3_client.upload_file(upload_path, '{}-resized'.format(bucket), 'resized-{}'.format(key))
        # s3_client.upload_file(download_path, bucket, 'resized-{}'.format(key))
        
        # Kiểm tra và xử lý file PDF
        file_extension = os.path.splitext(key)[1].lower()
        file_name = os.path.splitext(key)[0]

        if file_extension == '.pdf':
            print("Đây là file PDF.")
            pdf_path = "/tmp/" + file_name + ".pdf"
            output_dir = "/tmp/" + file_name
            output_file = "/tmp/" + file_name + ".json"
            
            # Tách PDF
            split_pdf(pdf_path, output_dir)
            
            # Xử lý từng file PDF nhỏ
            json_files = []
            for filename in os.listdir(output_dir):
                if filename.endswith(".pdf"):
                    pdf_file = os.path.join(output_dir, filename)
                    json_file = os.path.splitext(pdf_file)[0] + ".json"
                    extract_tables_to_json(pdf_file, json_file)
                    json_files.append(json_file)
            
            
            # Kết hợp các file JSON
            merge_json_files(json_files, output_file)
            file_size = os.path.getsize(output_file)
            print(f'Dung lượng file JSON vừa tạo: {file_size} bytes')
            
            # Import dữ liệu vào MongoDB
            json_file = output_file
            db_name = "checkvar"
            collection_name = "trans"
            mongo_uri = os.environ['MONGO_URI']
            import_json_to_mongodb(json_file, db_name, collection_name, mongo_uri)
        else:
            print("Đây không phải là file PDF.")