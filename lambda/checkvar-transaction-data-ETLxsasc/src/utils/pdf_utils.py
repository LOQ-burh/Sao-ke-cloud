import os
import PyPDF2
import pdfplumber
import json

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
        print('split_pdf output_file', output_file)


def extract_tables_to_json(pdf_path, output_file="table_data.json"):
  """
  Hàm trích xuất dữ liệu từ tất cả bảng trong PDF và lưu vào file JSON.

  Args:
      pdf_path: Đường dẫn đến tệp PDF.
      output_file: Tên file JSON để lưu kết quả (tùy chọn).
  """
  json_data = []
  with pdfplumber.open(pdf_path) as pdf:
    print('pdf_path', pdf_path, range(len(pdf.pages)))
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
            
  print('json_data', json_data)
  
  # Lưu JSON vào file
  with open(output_file, "w") as f:
    json.dump(json_data, f, indent=4)
    print('output_file', output_file)
    # json.dump(split_transactions(json_data), f, indent=4)


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