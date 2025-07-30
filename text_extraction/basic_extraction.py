# extracting/extractors.py
from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from .extraction_utils import validate_file, strip_html
from typing import List

import markdown

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
    file_extension = Path(file_path).suffix.lower().lstrip(".")
    for extractor in extractors:
        if file_extension in extractor.file_extensions:
            return extractor
    raise ValueError(f"No extractor found for file extension: {file_extension}")

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
        # validate file path and type
        file_path = validate_file(path)
        
        # Try different encodings
        for encoding in self.encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as file: #TODO:  errors='ignore'?
                    if file_path.suffix.lower() == ".xml":
                        return strip_html(file.read(), parser="xml")
                    
                    elif file_path.suffix.lower() == ".md":
                        text = markdown.markdown(file.read())
                        return strip_html(text, parser="html")

                    return file.read()
            except UnicodeDecodeError:
                continue
        
        # If we get here, none of the encodings worked
        raise ValueError(f"Unable to read file with supported encodings: {path}")