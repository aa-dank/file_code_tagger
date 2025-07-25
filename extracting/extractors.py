# extracting/extractors.py

from abc import ABC, abstractmethod
from pathlib import Path
from typing import List


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
        file_path = Path(path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Text file not found: {path}")
        
        if not file_path.is_file():
            raise FileNotFoundError(f"Path is not a file: {path}")
        
        # Try different encodings
        for encoding in self.encodings:
            try:
                with open(file_path, 'r', encoding=encoding) as file:
                    return file.read()
            except UnicodeDecodeError:
                continue
        
        # If we get here, none of the encodings worked
        raise ValueError(f"Unable to read file with supported encodings: {path}")

