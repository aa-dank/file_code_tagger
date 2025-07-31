# extracting/pdf_extractor.py

import fitz # PyMuPDF for PDF processing
import ocrmypdf
import os
import shutil
import tempfile

from pathlib import Path
from typing import Union
from .basic_extraction import FileTextExtractor
from .extraction_utils import validate_file, normalize_whitespace

class PDFFile:
    """
    Represents a PDF file and provides properties and utilities
    to inspect its content and layout.

    Attributes
    ----------
    path : Path
        Filesystem path to the PDF.
    name : str
        File name without its extension.
    page_count : int
        Total number of pages in the document.
    is_encrypted : bool
        True if the PDF is encrypted.
    property_cache : dict
        Cache for storing computed properties (e.g., page dimensions).

    Methods
    -------
    pt_to_in(pt: float) -> float
        Convert a measurement from PDF points to inches.
    _is_large_format_page(w: float, h: float, long_edge_thresh: int = 24,
                          area_thresh: int = 800) -> bool
        Determine if a page size in inches exceeds large‐format thresholds.

    Properties
    ----------
    pages_dims : List[Tuple[float, float]]
        List of (width, height) of each page in inches.
    has_large_format : bool
        True if any page qualifies as a large‐format page.
    """
    
    def __init__(self, path: str):
        """
        Initialize a PDFFile instance.

        Parameters
        ----------
        path : str
            Path to the PDF file.

        Raises
        ------
        FileNotFoundError
            If the path does not exist or is not a file.
        ValueError
            If the file cannot be opened as a PDF.
        """
        self.path = Path(path)
        # if the path doesn't exist or is not a file, raise an error
        if not self.path.exists(): 
            raise FileNotFoundError(f"PDF file not found: {path}")

        if not self.path.is_file():
            raise FileNotFoundError(f"PDF file is not a file: {path}")

        with fitz.open(self.path) as doc:
            # if the file is not a PDF, raise an error
            if doc.is_pdf is False:
                raise ValueError(f"File is not a valid PDF: {self.path}")
            self.page_count = doc.page_count
            self.is_encrypted = doc.is_encrypted

        self.name = self.path.stem
        self.property_cache = {}

    @staticmethod
    def _is_large_format_page(w, h, long_edge_thresh=24, area_thresh=800):
        """
        Check if a page size exceeds defined large-format thresholds.

        Parameters
        ----------
        w : float
            Width of the page in inches.
        h : float
            Height of the page in inches.
        long_edge_thresh : int, optional
            Minimum longer-edge length to consider large format (default=24).
        area_thresh : int, optional
            Minimum page area in square inches to consider large format (default=800).

        Returns
        -------
        bool
            True if page is large format, False otherwise.
        """
        long_edge = max(w, h)
        area = w * h
        return long_edge >= long_edge_thresh or area >= area_thresh
    
    @staticmethod
    def pt_to_in(pt: float) -> float:
        """
        Convert points to inches.
        
        Parameters
        ----------
        pt : float
            Value in points to convert.
        
        Returns
        -------
        float
            Value in inches.
        """
        return pt / 72.0

    @property
    def pages_dims(self) -> list:
        """
        Returns the dimensions of each page in inches.
        
        Returns
        -------
        list of tuples
            A list of tuples where each tuple contains the width and height of a page in inches.
        """
        if 'pages_dims' in self.property_cache:
            return self.property_cache['pages_dims']
        
        with fitz.open(self.path) as doc:
            dims = [(self.pt_to_in(page.rect.width), self.pt_to_in(page.rect.height)) for page in doc]
            self.property_cache['pages_dims'] = dims
        
        return self.property_cache['pages_dims']

    @property
    def has_large_format(self) -> bool:
        """
        Determine if any page in the PDF is large format.

        Returns
        -------
        bool
            True if at least one page qualifies as large format.
        """
        #if propert_cache has the value, return it
        if 'has_large_format' in self.property_cache:
            return self.property_cache['has_large_format']
        
        for w, h in self.pages_dims:
            if self._is_large_format_page(w, h):
                self.property_cache['has_large_format'] = True
                break

        if not 'has_large_format' in self.property_cache:
            self.property_cache['has_large_format'] = False

        return self.property_cache.get('has_large_format', False)

        
    

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
        # validate input file
        p = validate_file(pdf_filepath)
        pdf_text = ""
        ocr_needed = False
        with tempfile.TemporaryDirectory(prefix="text_extractor_") as staging_location:
            new_pdf_path = Path(staging_location) / p.name
            # copy the PDF to the staging location
            shutil.copy(str(p), new_pdf_path)
            pdf_file = PDFFile(new_pdf_path)

            # if the PDF is encrypted, raise an error
            if pdf_file.is_encrypted:
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

            with fitz.open(pdf_file.path) as doc:  # Updated to use the new read method
                for _, page in enumerate(doc):
                    page_text = page.get_text()
                    
                    # if we are not finding text, we'll attempt ocr
                    if not page_text.strip():
                        ocr_needed = True
                        break
                    pdf_text += page_text

        if ocr_needed:
            # perform OCR fallback
            pdf_text = self.extract_text_with_ocr(new_pdf_path)

        # normalize whitespace before returning
        return normalize_whitespace(pdf_text)