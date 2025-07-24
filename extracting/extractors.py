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
    
class PDFTextExtractor(FileTextExtractor):
    """
    Extract text from PDF files.
    
    This class implements the __call__ method to extract text from a PDF file.
    """

    def __init__(self, ocr_timeout=300):
        super().__init__()
        self.staging_location = tempfile.mkdtemp(prefix="pdf_text_extractor_")
        self.ocr_timeout = ocr_timeout

    def _get_pdf_page_count(self, pdf_path: str) -> int:
        """
        Get the number of pages in a PDF file.
        
        Parameters
        ----------
        pdf_path : str
            Path to the PDF file.
        
        Returns
        -------
        int
            Number of pages in the PDF.
        """
        with fitz.open(pdf_path) as doc:
            return doc.page_count
    
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

    def __call__(self, pdf_path: str) -> str:
        
        pdf_path = Path(pdf_path)
        pdf_text = ""
        ocr_empty_page_threshold = 0
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF file not found: {pdf_path}")
        new_pdf_path = Path(self.staging_location) / pdf_path.name
        # move the PDF to the staging location
        shutil.copy(pdf_path, new_pdf_path)
        pdf_page_count = self._get_pdf_page_count(new_pdf_path)
        
        # if the PDF has more than a page, raise the threshold for using ocr
        if pdf_page_count > 1:
            # must finsd text on first couple pages or we will use OCR
            ocr_empty_page_threshold = 1

        with fitz.open(new_pdf_path) as doc:
            for page_num, page in enumerate(doc):
                page_text = page.get_text()
                
                # if we are not finding text, we'll attempt ocr
                if not page_text.strip() and not pdf_text and page_num == ocr_empty_page_threshold:
                    pdf_text = self._ocr_pdf(new_pdf_path)
                    break
                pdf_text += page_text
        
        return pdf_text