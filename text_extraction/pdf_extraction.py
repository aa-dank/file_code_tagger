# extracting/pdf_extractor.py

import fitz # PyMuPDF for PDF processing
import ocrmypdf
import os
import shutil
import tempfile

from pathlib import Path
from typing import Union
from .basic_extraction import FileTextExtractor

class PDFFile:
    """
    Represents a PDF file and provides methods to access its properties.

    Attributes
    ----------
    path : Path
        The path to the PDF file.
    name : str
        The name of the PDF file without the extension.
    page_count : int
        The number of pages in the PDF file.
    encrypted : bool
        Indicates whether the PDF file is encrypted.
    pages_dims : list of tuples
        A list of tuples representing the dimensions of each page in inches (width, height).
    
    Methods
    -------
    read() -> fitz.Document
        Opens the PDF file and returns a fitz.Document object.
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
        with self.read() as doc:
            
            # if the file is not a PDF, raise an error
            if doc.is_pdf is False:
                raise ValueError(f"File is not a valid PDF: {self.path}")

            self.page_count = doc.page_count
            self.encrypted = doc.is_encrypted
            self.pages_dims = [(pt_to_in(page.rect.width), pt_to_in(page.rect.height)) for page in doc]

    @staticmethod
    def _is_large_format_page(w, h, long_edge_thresh=24, area_thresh=800):
        long_edge = max(w, h)
        area = w * h
        return long_edge >= long_edge_thresh or area >= area_thresh
    
    @property
    def has_large_format(self) -> bool:
        if not hasattr(self, '_has_large_format_pages'):
            self._has_large_format_pages = any(self._is_large_format_page(w, h) for w, h in self.pages_dims)
        return self._has_large_format_pages

    def read(self) -> fitz.Document:
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
    Extract text from PDF files with fallback to OCR.
    
    This class implements text extraction from PDF documents. It first attempts
    to extract text directly from the PDF. If no text is found (e.g., in scanned
    documents), it automatically falls back to OCR processing using ocrmypdf.
    
    Attributes
    ----------
    ocr_params : dict
        Parameters for OCR processing using ocrmypdf.
    """
    file_extensions = ['pdf']

    def __init__(self):
        super().__init__()
        self.ocr_params = {
            'max_image_mpixels': 250,
            'rotate_pages': True,
            'deskew': True,
            'invalidate_digital_signatures': True,
            'skip_text': True,
            'language': 'eng',
            'jobs': max(os.cpu_count() - 1, 1),  # Use all but one CPU core for OCR
            'optimize':1
        }

    def extract_text_with_ocr(self, pdf_path: Union[str, Path]) -> str:
        """
        Perform OCR on a PDF file and return the extracted text.
        
        This method uses ocrmypdf to process PDFs that don't have extractable text,
        such as scanned documents. It creates a new PDF with an OCR text layer
        and then extracts that text.
        
        Parameters
        ----------
        pdf_path : Union[str, Path]
            Path to the PDF file to be processed with OCR.
        
        Returns
        -------
        str
            Extracted text from the OCR-processed PDF.
            
        Raises
        ------
        FileNotFoundError
            If the input PDF file does not exist.
        """
        input_pdf_path = Path(pdf_path)
        pdf_text = ""
        if not input_pdf_path.exists():
            raise FileNotFoundError(f"Input PDF file not found for OCR operation: {input_pdf_path}")
        
        # staging location is directory containing the input_pdf_path file
        staging_location = input_pdf_path.parent
        output_pdf_path = Path(staging_location) / "ocr_output.pdf"
        ocr_params = self.ocr_params.copy()
        
        # add input and output file paths to the OCR parameters
        ocr_params['input_file'] = pdf_path
        ocr_params['output_file'] = output_pdf_path
        ocrmypdf.ocr(**ocr_params)

        with fitz.open(output_pdf_path) as doc:
            for page in doc:
                page_text = page.get_text()
                pdf_text += page_text

        return pdf_text

    def __call__(self, pdf_filepath: str) -> str:
        """
        Extract text from a PDF file, using OCR if necessary.
        
        This method first attempts to extract text directly from the PDF.
        If a page is found with no extractable text, it switches to OCR processing
        for the entire document.
        
        Parameters
        ----------
        pdf_filepath : str
            Path to the PDF file from which to extract text.
            
        Returns
        -------
        str
            Extracted text content from the PDF.
            
        Raises
        ------
        ValueError
            If the PDF is encrypted and cannot be processed.
        FileNotFoundError
            If the PDF file does not exist or is not a valid file.
        """
        pdf_text = ""
        ocr_needed = False
        with tempfile.TemporaryDirectory(prefix="text_extractor_") as staging_location:
            new_pdf_path = Path(staging_location) / Path(pdf_filepath).name
            # move the PDF to the staging location
            shutil.copy(pdf_filepath, new_pdf_path)
            pdf_file = PDFFile(new_pdf_path)

            # if the PDF is encrypted, raise an error
            if pdf_file.encrypted:
                raise ValueError(f"PDF file is encrypted and cannot be processed: {pdf_file.path}")
            
            # if no timeout param in ocr_params, set a default based on page count
            if not self.ocr_params.get('tesseract_timeout', None):
                self.ocr_params['tesseract_timeout'] = min(300, pdf_file.page_count * 45)

            # set the max_image_mpixels if not in ocr_params
            if not self.ocr_params.get('max_image_mpixels', None):
                
                if pdf_file.has_large_format:
                    self.ocr_params['max_image_mpixels'] = 1000
                    #self.ocr_params.setdefault("tesseract_config", []).extend(["--psm", "6"])
                else:
                    self.ocr_params['max_image_mpixels'] = 300

            with pdf_file.read() as doc:  # Updated to use the new read method
                for page_num, page in enumerate(doc):
                    page_text = page.get_text()
                    
                    # if we are not finding text, we'll attempt ocr
                    if not page_text.strip():
                        ocr_needed = True
                        break
                    pdf_text += page_text

        if ocr_needed:
            pdf_text = self.extract_text_with_ocr(new_pdf_path, staging_location)

        return pdf_text