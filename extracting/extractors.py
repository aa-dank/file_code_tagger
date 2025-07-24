# extracting/extractors.py

import fitz # PyMuPDF for PDF processing
import ocrmypdf
import shutil
import tempfile
from pathlib import Path
from abc import ABC, abstractmethod


class FileTextExtractor(ABC):
    
    @abstractmethod
    def __call__(self, *args, **kwds):
        raise NotImplementedError("Subclasses must implement this method.")

class PDFFile:
    """
    Represents a PDF file with its path and text content.
    
    Attributes:
        path (Path): The path to the PDF file.
        text (str): The extracted text from the PDF.
    """
    
    def __init__(self, path: str):
        
        def pt_to_in(pt: float) -> float:
            """Converts points to inches."""
            return pt / 72.0

        # if the path doesn't exist or is not a file, raise an error
        if not Path(path).exists(): 
            raise FileNotFoundError(f"PDF file not found: {path}")

        if not Path(path).is_file():
            raise FileNotFoundError(f"PDF file is not a file: {path}")

        self.path = Path(path)
        self.name = self.path.stem
        self.page_count = None
        self.encrypted = False
        self.pages_dims = []
        with fitz.open(self.path) as doc:
            
            # if the file is not a PDF, raise an error
            if doc.is_pdf is False:
                raise ValueError(f"File is not a valid PDF: {self.path}")

            self.page_count = doc.page_count
            self.encrypted = doc.is_encrypted
            self.pages_dims = [(pt_to_in(page.rect.width), pt_to_in(page.rect.height)) for page in doc]

    def read(self):
        """
        Open the PDF file and return the document object.
        
        Returns
        -------
        fitz.Document
            The opened PDF document.
        """
        return fitz.open(self.path)

class PDFTextExtractor(FileTextExtractor):
    """
    Extract text from PDF files.
    
    This class implements the __call__ method to extract text from a PDF file.
    """

    def __init__(self, ocr_timeout=300):
        super().__init__()
        self.staging_location = tempfile.mkdtemp(prefix="pdf_text_extractor_")
        self.ocr_timeout = ocr_timeout

    def _ocr_pdf(self, pdf_path: str) -> str:
        """
        Perform OCR on a PDF file and return the text.
        
        Parameters
        ----------
        pdf_path : str
            Path to the PDF file to be processed.
        
        Returns
        -------
        str
            Extracted text from the PDF.
        """
        ocr_output_path = Path(self.staging_location) / "ocr_output.pdf"
        ocrmypdf.ocr(pdf_path, ocr_output_path, timeout=self.ocr_timeout)
        
        with fitz.open(ocr_output_path) as doc:
            text = ""
            for page in doc:
                text += page.get_text()
        
        return text.strip()

    def __call__(self, pdf_filepath: str) -> str:
        pdf_text = ""
        ocr_empty_page_threshold = 0
        new_pdf_path = Path(self.staging_location) / Path(pdf_filepath).name
        # move the PDF to the staging location
        shutil.copy(pdf_filepath, new_pdf_path)
        pdf_file = PDFFile(new_pdf_path)

        # if the PDF has more than a page, raise the threshold for using ocr
        if pdf_file.page_count > 1:
            # must find text on first couple pages or we will use OCR
            ocr_empty_page_threshold = 1

        # if the PDF is encrypted, raise an error
        if pdf_file.encrypted:
            raise ValueError(f"PDF file is encrypted and cannot be processed: {pdf_file.path}")

        with pdf_file.read() as doc:  # Updated to use the new read method
            for page_num, page in enumerate(doc):
                page_text = page.get_text()
                
                # if we are not finding text, we'll attempt ocr
                if not page_text.strip() and not pdf_text and page_num == ocr_empty_page_threshold:
                    pdf_text = self._ocr_pdf(new_pdf_path)
                    break
                pdf_text += page_text
        
        return pdf_text