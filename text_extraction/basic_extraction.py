# extracting/extractors.py

import httpx
import logging
import os
import markdown
from abc import ABC, abstractmethod
from pathlib import Path
from .extraction_utils import validate_file, strip_html
from typing import List

logger = logging.getLogger(__name__)

class FileTextExtractor(ABC):
    """
    Abstract base class for text extraction from different file types.
    
    This class defines the interface for all text extractors. Subclasses should
    implement the __call__ method to handle specific file formats.
    """
    file_extensions: List[str] = None  # Class variable to define supported file extensions

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.file_extensions is None:
            raise TypeError(f"Class {cls.__name__} must define 'file_extensions' class variable")
        
    @abstractmethod
    def __call__(self, path: str) -> str:
        """
        Extract text content from a file.
        
        Parameters
        ----------
        path : str
            Path to the file from which to extract text.
            
        Returns
        -------
        str
            Extracted text content from the file.
        
        Raises
        ------
        NotImplementedError
            If the subclass does not implement this method.
        """
        raise NotImplementedError("Subclasses should implement this method.")
    

class TextFileTextExtractor(FileTextExtractor):
    """
    Extract text from plain text files.
    
    This class implements text extraction from various text-based file formats
    like .txt, .md, .csv, etc. It handles different encodings and provides
    basic error handling.
    """
    file_extensions = ['txt', 'md', 'log', 'csv', 'json', 'xml', 'yaml', 'yml', 'ini', 'cfg', 'conf']
    
    def __init__(self):
        super().__init__()
        self.encodings = ['utf-8', 'latin-1', 'cp1252', 'ascii']
    
    def __call__(self, path: str) -> str:
        """
        Extract text content from a plain text file.
        
        Parameters
        ----------
        path : str
            Path to the text file from which to extract text.
            
        Returns
        -------
        str
            Extracted text content from the file.
            
        Raises
        ----
        FileNotFoundError
            If the text file does not exist.
        ValueError
            If the file cannot be read with any of the supported encodings.
        """
        logger.info(f"Extracting text from file: {path}")
        # validate file path and type
        file_path = validate_file(path)
        logger.debug(f"Validated file path: {file_path}")
        
        # Try different encodings
        for encoding in self.encodings:
            logger.debug(f"Trying encoding: {encoding} for file: {file_path}")
            try:
                with open(file_path, 'r', encoding=encoding) as file: #TODO:  errors='ignore'?
                    if file_path.suffix.lower() == ".xml":
                        logger.debug(f"Stripping XML content from file: {file_path}")
                        return strip_html(file.read(), parser="xml")
                    
                    elif file_path.suffix.lower() == ".md":
                        logger.debug(f"Converting Markdown to HTML for file: {file_path}")
                        text = markdown.markdown(file.read())
                        return strip_html(text, parser="html")

                    return file.read()
            except UnicodeDecodeError:
                continue
        
        # If we get here, none of the encodings worked
        raise ValueError(f"Unable to read file with supported encodings: {path}")


class TikaUnsupportedError(Exception):
    """Raised when Tika cannot process a file due to unsupported format or encryption."""
    def __init__(self, filepath: str, message: str = "Unsupported by Tika"):
        self.filepath = filepath
        super().__init__(f"{message}: {filepath}")


class TikaNoContentError(Exception):
    """Raised when Tika returns 204 No Content (e.g., image‐only file without OCR)."""
    def __init__(self, filepath: str, message: str = "No content found"):
        self.filepath = filepath
        super().__init__(f"{message}: {filepath}")


class TikaTextExtractor(FileTextExtractor):
    """
    Fallback extractor using a containerized Apache Tika (REST API).
    """
    # catch‐all for most formats; register this last in your extractor list
    file_extensions = [
        'pdf','doc','docx','ppt','pptx','xls','xlsx','rtf',
        'html','htm','txt','csv','xml','json','md',
        'png','jpg','jpeg','gif','tif','tiff','eml','msg',
        'odt','ods'
    ]

    def __init__(self, server_url: str | None = None, timeout: int = 60):
        super().__init__()
        # e.g. "http://localhost:9998"
        self.server_url = server_url or os.environ.get('TIKA_SERVER_URL', 'http://localhost:9998')
        self.tika_endpoint = f"{self.server_url}/tika"
        self.detect_endpoint = f"{self.server_url}/detect/stream"
        self.timeout = timeout

        # sanity check server is up
        r = httpx.get(self.tika_endpoint, headers={'Accept': 'text/plain'}, timeout=self.timeout)
        r.raise_for_status()

    def _detect_mime(self, path: Path) -> str:
        # filename hint improves detection
        with open(path, 'rb') as fh:
            r = httpx.put(
                self.detect_endpoint,
                content=fh,
                headers={'Content-Disposition': f'attachment; filename=\"{path.name}\"'},
                timeout=self.timeout
            )
        r.raise_for_status()
        return (r.text or '').strip()

    def __call__(self, path: str) -> str:
        p = validate_file(path)
        # Preflight: detect MIME
        mime = self._detect_mime(p)
        logger.debug(f"Tika detected MIME for {p}: {mime or 'UNKNOWN'}")

        # Fast-fail on clearly unknown/opaque types
        if not mime or mime == 'application/octet-stream':
            raise TikaUnsupportedError(f"Tika can’t determine a usable MIME type for {p}")

        logger.info(f"Extracting text from {p} with Tika (MIME={mime})")
        # Extract text
        with open(p, 'rb') as fh:
            resp = httpx.put(
                self.tika_endpoint,
                content=fh,
                headers={'Accept': 'text/plain'},
                timeout=self.timeout
            )

        # Explicit handling of common outcomes
        if resp.status_code == 204:
            raise TikaNoContentError(f"Tika returned 204 No Content for {p}")
        if resp.status_code == 422:
            raise TikaUnsupportedError(f"Tika returned 422 (unsupported/encrypted) for {p}: {resp.text}")

        resp.raise_for_status()

        text = resp.text or ""
        if not text.strip():
            logger.warning(f"Tika returned 200 but empty body for {p} (MIME={mime})")
        return text
    
def get_extractor_for_file(file_path: str, extractors: list) -> FileTextExtractor:
    """
    Determine the appropriate extractor for a given file based on its extension.

    Parameters
    ----------
    file_path : str
        Path to the file to be processed.
    extractors : list
        List of extractor instances.

    Returns
    -------
    FileTextExtractor
        The extractor instance that matches the file extension.

    Raises
    ------
    ValueError
        If no extractor matches the file extension.
    """
    logger.debug(f"Finding extractor for file: {file_path}")
    file_extension = Path(file_path).suffix.lower().lstrip(".")
    for extractor in extractors:
        if file_extension in extractor.file_extensions:
            logger.debug(f"Selected extractor {extractor.__class__.__name__} for file: {file_path}")
            return extractor
    logger.error(f"No extractor found for file extension: {file_extension}")
    return None