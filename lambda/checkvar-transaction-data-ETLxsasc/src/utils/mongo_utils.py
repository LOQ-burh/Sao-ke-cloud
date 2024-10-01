from pymongo import MongoClient
import json

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