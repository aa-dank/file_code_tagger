# extracting/pdf_extractor.py

import fitz
import logging

# Raise the maximum number of pixels for images to prevent errors with large PDFs
try:
    from PIL import Image
    Image.MAX_IMAGE_PIXELS = 300_000_000  # or None to disable
except ImportError:
    pass

import io
import ocrmypdf
import os
import shutil
import tempfile

from pathlib import Path
from typing import Union, List
from .basic_extraction import FileTextExtractor
from .extraction_utils import validate_file

logger = logging.getLogger(__name__)

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
        logger.debug(f"Initializing PDFFile for path: {path}")
        self.path = Path(path)
        # if the path doesn't exist or is not a file, raise an error
        if not self.path.exists(): 
            logger.error(f"PDF file not found: {self.path}")
            raise FileNotFoundError(f"PDF file not found: {path}")

        if not self.path.is_file():
            logger.error(f"PDF path is not a file: {self.path}")
            raise FileNotFoundError(f"PDF file is not a file: {path}")

        with fitz.open(self.path) as doc:
            # if the file is not a PDF, raise an error
            if not doc.is_pdf:
                logger.error(f"File is not a valid PDF: {self.path}")
                raise ValueError(f"File is not a valid PDF: {self.path}")
            
            self.page_count = doc.page_count
            self.is_encrypted = doc.is_encrypted
        logger.debug(f"PDFFile {self.path} has {self.page_count} pages; encrypted={self.is_encrypted}")

        self.name = self.path.stem
        self.size = self.path.stat().st_size  # size in bytes
        # cache for properties that are expensive to compute and not used much
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
        logger.debug(f"Computing pages dimensions for {self.path}")
        
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
        
        logger.debug(f"Checking for large format pages in {self.path}")
        for w, h in self.pages_dims:
            if self._is_large_format_page(w, h):
                logger.debug(f"Page with size {w}x{h} inches is large format")
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
    file_extensions : list
        Supported file extensions for this extractor.
    ocr_params : dict
        Parameters for OCR processing using ocrmypdf.
    max_stream_size : int
        Maximum file size (bytes) to process in memory before using a temp file.
    """
    file_extensions = ['pdf']

    def __init__(self):
        """
        Initialize PDFTextExtractor with default OCR parameters and stream-size threshold.
        """
        super().__init__()
        self.ocr_params = {
            'max_image_mpixels': 250,
            'rotate_pages': True,
            'deskew': True,
            'invalidate_digital_signatures': True,
            'skip_text': True,
            'language': 'eng',
            'jobs': max(os.cpu_count() - 1, 1),  # Use all but one CPU core for OCR
            'optimize': 0,
            'output_type': 'pdf',
            'tesseract_timeout': 300,  # default timeout for Tesseract OCR
        }

        # threshold of files which cannot be processed in memory, default is 100 MB
        self.max_stream_size = 100 * 1024 * 1024
    
    @staticmethod
    def extract_text_with_ocr(pdf_path: Union[str, Path], ocr_params: dict) -> str:
        """
        Perform OCR on a PDF file and return the extracted text.
        
        This method uses ocrmypdf to process PDFs that don't have extractable text,
        such as scanned documents. It creates a new PDF with an OCR text layer
        and then extracts that text.
        
        Parameters
        ----------
        pdf_path : Union[str, Path]
            Path to the PDF file to be processed with OCR.
        ocr_params : dict
            Parameters for the OCR processing.

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
        logger.debug(f"Starting OCR extraction for {input_pdf_path} with params: {ocr_params}")
        if not input_pdf_path.exists():
            raise FileNotFoundError(f"Input PDF file not found for OCR operation: {input_pdf_path}")
        
        with tempfile.TemporaryDirectory(prefix="ocr_") as td:
            # staging location is directory containing the input_pdf_path file
            output_pdf_path = Path(td) / f"{input_pdf_path.stem}_ocr.pdf"

            # add input and output file paths to the OCR parameters
            params = ocr_params.copy()
            params['input_file'] = pdf_path
            params['output_file'] = output_pdf_path
            ocrmypdf.ocr(**params)
            logger.debug(f"OCR completed, reading text from generated PDF")

            with fitz.open(output_pdf_path) as doc:
                return "".join(page.get_text() for page in doc)
    
    def _fitz_doc_text(self, fitz_doc: fitz.Document, pdf_document: PDFFile) -> str:
        """
        Extract text from a fitz.Document, with fallback to OCR if any page is blank.

        Parameters
        ----------
        fitz_doc : fitz.Document
            Opened PyMuPDF document.
        pdf_document : PDFFile
            PDFFile instance for metadata and page count.

        Returns
        -------
        str
            Extracted text, using OCR if necessary.
        """
        logger.debug(f"Extracting text with fitz for document: {pdf_document.path}")
        ocr_needed_length_threshold = 100 # if found text is less than this, trigger OCR
        pdf_text = ""
        for _, page in enumerate(fitz_doc):
            page_text = page.get_text()
            pdf_text += page_text
        
        if len(pdf_text) >= ocr_needed_length_threshold:
            logger.debug(f"Extracted text length {len(pdf_text)}.")
            return pdf_text
        
        logger.info(f"OCR needed for document: {pdf_document.path}")
        ocr_params = self.ocr_params.copy()
        # if no timeout param in ocr_params, set a default based on page count
        if not ocr_params.get('tesseract_timeout', None):
            ocr_params['tesseract_timeout'] = min(300, pdf_document.page_count * 45)

        # set the max_image_mpixels if not in ocr_params
        if not ocr_params.get('max_image_mpixels', None):
            ocr_params['max_image_mpixels'] = 1000 if pdf_document.has_large_format else 300

        pdf_text = self.extract_text_with_ocr(pdf_path=pdf_document.path, ocr_params=ocr_params)
        return pdf_text

    def __call__(self, pdf_filepath: str) -> str:
        """
        Extract and normalize text from the specified PDF file.

        Parameters
        ----------
        pdf_filepath : str
            Filesystem path to the PDF to process.

        Returns
        -------
        str
            Normalized extracted text.
        """
        
        # Initialize document handle and result container
        logger.debug(f"__call__: Starting extraction for file {pdf_filepath}")
        doc = None
        extracted_text = ""
        try:
            validated = validate_file(pdf_filepath)
            # Log validated path
            logger.debug(f"__call__: validated file path {validated}")
            pdf = PDFFile(validated)
            # Log PDF metadata
            logger.debug(f"__call__: PDF metadata size={pdf.size}, pages={pdf.page_count}, encrypted={pdf.is_encrypted}")

            # PyMuPDF can open encrypted PDFs only with a password; streaming doesn't help.
            if pdf.is_encrypted:
                logger.warning(f"PDF is encrypted, cannot extract text: {pdf.name}")
                raise ValueError(f"PDF file is encrypted and cannot be processed: {pdf.name}")
            
            # if the file is small enough, read it into memory
            if pdf.size <= self.max_stream_size:
                logger.debug(f"PDF size {pdf.size} <= max_stream_size ({self.max_stream_size}), processing in-memory")
                data = pdf.path.read_bytes()
                doc = fitz.open(stream=data, filetype="pdf")
                extracted_text = self._fitz_doc_text(fitz_doc=doc, pdf_document=pdf)
                doc.close()

            else:
                logger.debug(f"PDF size {pdf.size} > max_stream_size ({self.max_stream_size}), processing via temp file")
                with tempfile.TemporaryDirectory(prefix="text_extractor_") as temp_dir:
                    work_path = Path(temp_dir) / pdf.name
                    shutil.copy(pdf.path, work_path)
                    doc = fitz.open(work_path)
                    extracted_text = self._fitz_doc_text(fitz_doc=doc, pdf_document=pdf)
                    doc.close()
        
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf.name}: {e}")
            raise e

        finally:
            if doc is not None and not doc.is_closed:
                try:
                    doc.close()
                except Exception as e:
                    pass

        # Final debug before returning
        logger.debug(f"__call__: extraction complete, returning {len(extracted_text)} characters")
        return extracted_text
    

class PDFTextExtractor2(FileTextExtractor):
    """
    Extract text from PDF files with fallback to OCR.

    This class implements text extraction from PDF documents. It first attempts
    to extract text directly from the PDF. For continuous sequences of pages without
    text, it falls back to OCR processing using ocrmypdf.
 
    Attributes
    ----------
    file_extensions : list
        Supported file extensions for this extractor.
    ocr_params : dict
        Parameters for OCR processing using ocrmypdf.
    max_stream_size : int
        Maximum file size (bytes) to process in memory before using a temp file.
    """
    file_extensions = ['pdf']

    def __init__(self):
        """
        Initialize PDFTextExtractor2 with default OCR parameters and stream-size threshold.

        This sets up the OCR configuration and maximum in-memory file size.
        """
        # Log initialization start
        logger.debug("__init__: Starting initialization of PDFTextExtractor2")
        super().__init__()
        # Use output_type 'pdf' so a temporary OCR'd PDF is produced; avoids stdout TTY issues.
        self.ocr_params = {
            'max_image_mpixels': 250,
            'rotate_pages': True,
            'deskew': True,
            'invalidate_digital_signatures': True,
            'skip_text': True,
            'language': 'eng',
            'jobs': max(os.cpu_count() - 1, 1),  # Use all but one CPU core for OCR
            'optimize': 0,
            'output_type': 'pdf',
            'tesseract_timeout': 300,  # default timeout for Tesseract OCR
            'progress_bar': False
        }

        # threshold of files which cannot be processed in memory, default is 100 MB
        self.max_stream_size = 100 * 1024 * 1024
        # Log initialization details
        logger.debug(f"Initialized PDFTextExtractor2 with max_stream_size={self.max_stream_size} and default OCR params keys={list(self.ocr_params.keys())}")
            
    def _identify_blank_ranges(self, page_texts: List[str]) -> List[range]:
        """
        Identify contiguous ranges of pages without native text.

        Parameters
        ----------
        page_texts : List[str]
            Extracted text for each page.

        Returns
        -------
        List[range]
            Ranges of page indices representing blank sequences.
        """
        # Log the number of pages to inspect
        logger.debug(f"_identify_blank_ranges: processing {len(page_texts)} pages")
        blank_ranges = []
        current_range = None

        for i, text in enumerate(page_texts):
            if not text.strip():  # If the page is blank
                if current_range is None:
                    current_range = range(i, i + 1)
                else:
                    current_range = range(current_range.start, i + 1)
            else:
                if current_range is not None:
                    blank_ranges.append(current_range)
                    current_range = None

        if current_range is not None:
            blank_ranges.append(current_range)
        # Log found blank page ranges
        logger.debug(f"_identify_blank_ranges: found blank ranges {blank_ranges}")

        return blank_ranges

    def _get_pages_text_list(self, fitz_doc: fitz.Document, pdf_document: PDFFile) -> List[str]:
        """
        Extract native text from each page using PyMuPDF.

        Parameters
        ----------
        fitz_doc : fitz.Document
            Opened PDF document.
        pdf_document : PDFFile
            PDFFile instance for metadata.

        Returns
        -------
        List[str]
            Extracted text for each page.
        """
        # Log invocation
        logger.debug(f"_get_pages_text_list: extracting {pdf_document.page_count} pages from {pdf_document.path}")
        logger.info(f"Extracting text with fitz for document: {pdf_document.path}")
        pdf_pages_text = []
        for idx, page in enumerate(fitz_doc):
            page_text = page.get_text()
            pdf_pages_text.append(page_text)
            # Debug each page length and a short snippet
            snippet = page_text[:80].replace('\n', ' ') if page_text else ''
            logger.debug(f"_get_pages_text_list: page {idx} length={len(page_text)} snippet='{snippet[:80]}'")
        logger.debug(f"_get_pages_text_list: total native text chars={sum(len(t) for t in pdf_pages_text)}")

        return pdf_pages_text
    
    def _ocr_extract_blank_pages(self, pdf: PDFFile, blank_ranges: List[range]):
        """
        Perform OCR on specified blank page ranges and collect extracted text.

        Parameters
        ----------
        pdf : PDFFile
            PDFFile instance for the document.
        blank_ranges : List[range]
            Ranges of page indices lacking native text.

        Returns
        -------
        dict
            Mapping of page ranges to OCR text.
        """
        # Early exit if nothing to OCR
        if not blank_ranges:
            logger.debug("_ocr_extract_blank_pages: no blank ranges; skipping OCR")
            return {}

        logger.info(f"_ocr_extract_blank_pages: OCR for blank ranges {blank_ranges} in {pdf.path}")
        page_ocr_dict: dict[int, str] = {}
        full_pdf = Path(pdf.path)

        total_blank_pages = sum(len(r) for r in blank_ranges)
        failed_pages: list[int] = []
        with tempfile.TemporaryDirectory(prefix="ocr_batches_") as temp_dir:
            for r in blank_ranges:
                start, end = r.start, r.stop
                logger.debug(f"_ocr_extract_blank_pages: processing OCR for pages {start}-{end - 1}")
                try:
                    # Fresh params per range
                    ocr_call_params = self.ocr_params.copy()
                    if not ocr_call_params.get('max_image_mpixels', None):
                        ocr_call_params['max_image_mpixels'] = 1000 if pdf.has_large_format else 300
                    if not ocr_call_params.get('tesseract_timeout', None):
                        ocr_call_params['tesseract_timeout'] = min(300, (end - start) * 45)

                    subpdf_path = Path(temp_dir) / f"sub_{start}_{end}.pdf"
                    # Build sub-PDF containing the blank page range
                    with fitz.open(str(full_pdf)) as full_doc:
                        subdoc = fitz.open()
                        for i in range(start, end):
                            subdoc.insert_pdf(full_doc, from_page=i, to_page=i)
                        subdoc.save(subpdf_path)

                    sidecar_io = io.BytesIO()
                    # Run OCR capturing only sidecar text using stdout redirection workaround
                    self._run_ocr_sidecar_only(subpdf_path, sidecar_io, ocr_call_params)
                    sidecar_io.seek(0)
                    try:
                        ocr_text = sidecar_io.read().decode('utf-8', errors='ignore')
                    except Exception as dec_e:
                        logger.error(f"_ocr_extract_blank_pages: error decoding sidecar for pages {start}-{end - 1}: {dec_e}")
                        ocr_text = ""
                    if not ocr_text.strip():
                        logger.warning(f"_ocr_extract_blank_pages: empty OCR sidecar text for pages {start}-{end - 1}")
                    # Attempt to split multi-page OCR output into per-page segments
                    num_pages = end - start
                    import re as _re  # local import to respect 'only modify class' constraint
                    trimmed = ocr_text.rstrip('\f\x0c')
                    # Split on form feed characters which ocrmypdf uses between pages in sidecar output
                    raw_splits = [_s for _s in _re.split(r'[\f\x0c]+', trimmed) if _s is not None]
                    # Remove completely empty trailing segments
                    page_segments = [seg for seg in raw_splits if seg.strip()]
                    if len(page_segments) == num_pages:
                        for offset, seg in enumerate(page_segments):
                            page_index = start + offset
                            page_ocr_dict[page_index] = seg
                            logger.debug(f"_ocr_extract_blank_pages: assigned split OCR text to page {page_index} len={len(seg)}")
                    else:
                        logger.warning(
                            f"_ocr_extract_blank_pages: split page count mismatch (expected {num_pages}, got {len(page_segments)}) for pages {start}-{end - 1}; falling back to per-page OCR"
                        )
                        # Fallback: perform OCR one page at a time to avoid duplication
                        for single_page in range(start, end):
                            try:
                                single_subpdf = Path(temp_dir) / f"sub_{single_page}_{single_page+1}.pdf"
                                with fitz.open(str(full_pdf)) as full_doc:
                                    _single = fitz.open()
                                    _single.insert_pdf(full_doc, from_page=single_page, to_page=single_page)
                                    _single.save(single_subpdf)
                                per_page_sidecar = io.BytesIO()
                                self._run_ocr_sidecar_only(single_subpdf, per_page_sidecar, ocr_call_params)
                                per_page_sidecar.seek(0)
                                try:
                                    per_text = per_page_sidecar.read().decode('utf-8', errors='ignore')
                                except Exception as _dec2:
                                    logger.error(f"_ocr_extract_blank_pages: decode error page {single_page}: {_dec2}")
                                    per_text = ""
                                if not per_text.strip():
                                    logger.warning(f"_ocr_extract_blank_pages: empty OCR text after per-page fallback for page {single_page}")
                                page_ocr_dict[single_page] = per_text
                                logger.debug(f"_ocr_extract_blank_pages: per-page OCR assigned to page {single_page} len={len(per_text)}")
                            except Exception as _single_e:
                                logger.error(f"_ocr_extract_blank_pages: per-page OCR failed for page {single_page}: {_single_e}")
                                failed_pages.append(single_page)
                except Exception as ocr_e:
                    logger.error(f"_ocr_extract_blank_pages: OCR failed for pages {start}-{end - 1}: {ocr_e}")
                    failed_pages.extend(list(range(start, end)))
                    continue

        logger.info(f"_ocr_extract_blank_pages: OCR completed for {len(page_ocr_dict)} pages")
        # Attach metadata about failures so caller can decide to raise
        page_ocr_dict['_failed_pages'] = failed_pages  # type: ignore
        page_ocr_dict['_total_blank_pages'] = total_blank_pages  # type: ignore
        return page_ocr_dict
    
    def _compile_text(self, pages_text: List[str], ocr_texts: dict) -> str:
        """
        Compile native and OCR texts into a single combined string.

        Parameters
        ----------
        pages_text : List[str]
            Native extracted text per page.
        ocr_texts : dict
            Mapping of page indices/ranges to OCR text.

        Returns
        -------
        str
            Combined text for entire document.
        """
        # Log compiling details
        logger.debug(f"_compile_text: compiling text from {len(pages_text)} pages with OCR entries {len(ocr_texts)}")
        compiled_text = []
        for i, text in enumerate(pages_text):
            if text.strip():
                compiled_text.append(text)
            else:
                ocr_page_text = ocr_texts.get(i)
                if ocr_page_text:
                    compiled_text.append(ocr_page_text)
        return "\n".join(compiled_text)

    def _run_ocr_sidecar_only(self, input_path: Path, sidecar_io: io.BytesIO, params: dict) -> None:
        """Run ocrmypdf to populate only the sidecar text while suppressing stdout TTY issues.

        This forces output_type='none' and output_file='-' so that ocrmypdf writes the
        OCR PDF to stdout (discarded) and the extracted text to the provided sidecar
        BytesIO. A low-level stdout redirection (fd=1) to a temporary file ensures
        ocrmypdf does not error when stdout is not a real file.

        Parameters
        ----------
        input_path : Path
            Path to the input PDF file (or sub-PDF) to OCR.
        sidecar_io : io.BytesIO
            In-memory buffer to receive sidecar text output.
        params : dict
            Base OCR parameters to copy and augment.
        """
        copy_params = params.copy()
        copy_params['input_file'] = input_path
        # Create temp output PDF to satisfy ocrmypdf when producing an OCR layer
        with tempfile.TemporaryDirectory(prefix="ocr_out_") as td_out:
            output_pdf = Path(td_out) / "ocr_out.pdf"
            copy_params['output_file'] = output_pdf
            copy_params['sidecar'] = sidecar_io  # request sidecar text
            copy_params['output_type'] = 'pdf'
            try:
                ocrmypdf.ocr(**copy_params)
            except Exception as e:
                logger.error(f"_run_ocr_sidecar_only: ocrmypdf failed for {input_path}: {e}")
                raise
            # If sidecar remained empty, fallback: extract text from produced PDF
            if sidecar_io.getbuffer().nbytes == 0 and output_pdf.exists():
                try:
                    with fitz.open(output_pdf) as ocr_doc:
                        extracted = "".join(p.get_text() for p in ocr_doc)
                        sidecar_io.write(extracted.encode('utf-8', errors='ignore'))
                        logger.debug(f"_run_ocr_sidecar_only: sidecar empty; used OCR PDF text length={len(extracted)}")
                except Exception as fe:
                    logger.warning(f"_run_ocr_sidecar_only: fallback read failed for {output_pdf}: {fe}")

    def __call__(self, pdf_filepath: str) -> str:
        """
        Extract and normalize text from the specified PDF file.

        Parameters
        ----------
        pdf_filepath : str
            Filesystem path to the PDF to process.

        Returns
        -------
        str
            Normalized extracted text.
        """
        
        # Initialize document handle and result container
        logger.debug(f"__call__: Starting extraction for file {pdf_filepath}")
        doc = None
        extracted_text = ""
        try:
            validated = validate_file(pdf_filepath)
            # Log validated path
            logger.debug(f"__call__: validated file path {validated}")
            pdf = PDFFile(validated)
            # Log PDF metadata
            logger.debug(f"__call__: PDF metadata size={pdf.size}, pages={pdf.page_count}, encrypted={pdf.is_encrypted}")

            # PyMuPDF can open encrypted PDFs only with a password; streaming doesn't help.
            if pdf.is_encrypted:
                logger.warning(f"PDF is encrypted, cannot extract text: {pdf.name}")
                raise ValueError(f"PDF file is encrypted and cannot be processed: {pdf.name}")
            
            # if the file is small enough, read it into memory
            if pdf.size <= self.max_stream_size:
                logger.debug(f"PDF size {pdf.size} <= max_stream_size ({self.max_stream_size}), processing in-memory")
                data = pdf.path.read_bytes()
                doc = fitz.open(stream=data, filetype="pdf")
                extracted_text = self._get_pages_text_list(fitz_doc=doc, pdf_document=pdf)
                blank_ranges = self._identify_blank_ranges(extracted_text)
                blank_ranges_text = self._ocr_extract_blank_pages(pdf=pdf, blank_ranges=blank_ranges)
                if blank_ranges:
                    failed = blank_ranges_text.pop('_failed_pages', [])  # type: ignore
                    total_blank = blank_ranges_text.pop('_total_blank_pages', 0)  # type: ignore
                    if failed:
                        logger.warning(f"__call__: OCR failures for pages {failed} (blank set size={total_blank}); proceeding with native text where available.")
                extracted_text = self._compile_text(extracted_text, blank_ranges_text)
                doc.close()

            else:
                logger.debug(f"PDF size {pdf.size} > max_stream_size ({self.max_stream_size}), processing via temp file")
                with tempfile.TemporaryDirectory(prefix="text_extractor_") as temp_dir:
                    work_path = Path(temp_dir) / pdf.name
                    shutil.copy(pdf.path, work_path)
                    doc = fitz.open(work_path)
                    extracted_text = self._get_pages_text_list(fitz_doc=doc, pdf_document=pdf)
                    blank_ranges = self._identify_blank_ranges(extracted_text)
                    logger.debug(f"__call__: blank ranges identified {blank_ranges}")
                    blank_ranges_text = self._ocr_extract_blank_pages(pdf=pdf, blank_ranges=blank_ranges)
                    if blank_ranges:
                        failed = blank_ranges_text.pop('_failed_pages', [])  # type: ignore
                        total_blank = blank_ranges_text.pop('_total_blank_pages', 0)  # type: ignore
                        if failed:
                            logger.warning(f"__call__: OCR failures for pages {failed} (blank set size={total_blank}); proceeding with native text where available.")
                    extracted_text = self._compile_text(extracted_text, blank_ranges_text)
                    doc.close()
        
        except Exception as e:
            logger.error(f"Error extracting text from PDF {pdf.name}: {e}")
            raise e

        finally:
            if doc is not None and not doc.is_closed:
                try:
                    doc.close()
                except Exception as e:
                    logger.error(f"Error closing document {pdf.name}: {e}")
                    return extracted_text

        # Final debug before returning
        logger.debug(f"__call__: extraction complete, returning {len(extracted_text)} characters")
        return extracted_text
