from abc import ABC, abstractmethod

class FileProcessor(ABC):
    @abstractmethod
    def process(self, file_path):
        pass

class FileProcessorFactory:
    @staticmethod
    def get_processor(file_extension):
        #print('FileProcessorFactory', file_extension)
        if file_extension == '.pdf':
            from processors.pdf_processor import PDFProcessor
            return PDFProcessor()
        elif file_extension == '.json':
            from processors.json_processor import JSONProcessor
            return JSONProcessor()
        else:
            raise ValueError(f"Unsupported file type: {file_extension}")