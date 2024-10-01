import os
from utils.pdf_utils import split_pdf, extract_tables_to_json, merge_json_files
from processors.file_processor import FileProcessor

class PDFProcessor(FileProcessor):
    def process(self, file_path):
        output_dir = os.path.splitext(file_path)[0]
        output_file = os.path.splitext(file_path)[0] + ".json"
        print('PDFProcessor output_dir', output_dir)

        split_pdf(file_path, output_dir)

        json_files = []
        print('os.listdir(output_dir)', os.listdir(output_dir))
        for filename in os.listdir(output_dir):
            if filename.endswith(".pdf"):
                pdf_file = os.path.join(output_dir, filename)
                json_file = os.path.splitext(pdf_file)[0] + ".json"
                extract_tables_to_json(pdf_file, json_file)
                json_files.append(json_file)

        print('PDFProcessor json_files', json_files)
        merge_json_files(json_files, output_file)
        return output_file