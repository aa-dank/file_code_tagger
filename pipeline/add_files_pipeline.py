# pipeline/add_files_pipeline.py

import logging
import os
import shutil
import traceback
import tempfile
import pytesseract
from pathlib import Path
from typing import Optional
from db_models import File, FileLocation, FilingTag, FileTagLabel, FileEmbedding, get_db_engine
from embedding.minilm import MiniLMEmbedder
from sqlalchemy import func, literal
from sqlalchemy.orm import Session
from text_extraction.pdf_extraction import PDFTextExtractor
from text_extraction.basic_extraction import TextFileTextExtractor, TikaTextExtractor, get_extractor_for_file
from text_extraction.image_extraction import ImageTextExtractor
from text_extraction.office_doc_extraction import PresentationTextExtractor, SpreadsheetTextExtractor, WordFileTextExtractor
from text_extraction.web_extraction import HtmlTextExtractor, EmailTextExtractor
from text_extraction.extraction_utils import common_char_replacements, strip_diacritics, normalize_unicode, normalize_whitespace
from utils import extract_server_dirs

DEFAULT_MAX_SIZE_MB = 200
DEFAULT_TEXT_LENGTH_THRESHOLD = 250

# Initialize extractors and Tika fallback
pdf_extractor = PDFTextExtractor()
txt_extractor = TextFileTextExtractor()
image_extractor = ImageTextExtractor()
presentation_extractor = PresentationTextExtractor()
spreadsheet_extractor = SpreadsheetTextExtractor()
word_extractor = WordFileTextExtractor()
html_extractor = HtmlTextExtractor()
email_extractor = EmailTextExtractor()
tika_extractor = TikaTextExtractor()
extractors_list = [
    pdf_extractor,
    txt_extractor,
    image_extractor,
    presentation_extractor,
    spreadsheet_extractor,
    word_extractor,
    html_extractor,
    email_extractor,
]

def init_tesseract(cmd: Optional[str] = None):
    """Configure pytesseract to use a specific Tesseract executable if provided.

    Parameters:
        cmd (Optional[str]): Full path to tesseract binary; uses PATH if None.
    """
    # Set custom tesseract command if given
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd

def get_files_from_taggged_locations_query(
    db_session: Session,
    tag_obj: FilingTag,
    n: int = 100,
    randomize: bool = False,
    exclude_embedded: bool = False,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB
):
    """generates query for fetching file locations matching a filing tag with optional filters.

    - Joins File ⇄ FileLocation on matching tag directory
    - Optionally excludes already‐embedded files
    - Optionally filters by max size
    - Optionally shuffles and limits the result

    Returns:
        List[File]: ORM objects to process
    """
    tag_locations = FileLocation.file_server_directories.ilike(f"%/{tag_obj.full_tag_label_str}%")
    q = db_session.query(File)\
        .join(FileLocation)\
        .filter(tag_locations)
    if exclude_embedded:
        q = q.outerjoin(FileEmbedding, File.hash == FileEmbedding.file_hash)\
             .filter(FileEmbedding.file_hash == None)
    if max_size_mb is not None:
        max_bytes = max_size_mb * 1024 * 1024
        q = q.filter(File.size <= max_bytes)
    if randomize:
        q = q.order_by(func.random())
    q = q.limit(n)
    return q

def get_files_from_server_locations_query(
    db_session: Session,
    server_dirs: str | Path,
    n: int = None,
    randomize: bool = False,
    exclude_embedded: bool = False,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB
):
    """generates query for fetching file locations matching a server directory with optional filters.

    - Optionally excludes already‐embedded files
    - Optionally filters by max size
    - Optionally shuffles and limits the result

    Returns:
        Query: SQLAlchemy query object
    """
    files_located_in_dir = FileLocation.file_server_directories.startswith(server_dirs)
    q = db_session.query(File)\
        .join(FileLocation)\
        .filter(files_located_in_dir)
    if exclude_embedded:
        q = q.outerjoin(FileEmbedding, File.hash == FileEmbedding.file_hash)\
             .filter(FileEmbedding.file_hash == None)
    if max_size_mb is not None:
        max_bytes = max_size_mb * 1024 * 1024
        q = q.filter(File.size <= max_bytes)
    if randomize:
        q = q.order_by(func.random())
    if n is not None:
        q = q.limit(n)
    return q

def label_file_using_tag(
    db_session: Session,
    file_obj: File,
    some_tag: FilingTag | str,
    label_source: str = 'rule'
):
    """Assign a filing tag (and its ancestors) to a File record.

    Inserts only missing labels and commits at the end.

    Parameters:
        db_session (Session): Active SQLAlchemy session
        file_obj (File): Target File ORM instance
        some_tag (FilingTag | str): Tag instance or label string
        label_source (str): Origin of the label ('human','rule','model')
    """
    if isinstance(some_tag, str):
        tag_obj = FilingTag.retrieve_tag_by_label(db_session, some_tag)
        if not tag_obj:
            raise ValueError(f"Tag '{some_tag}' not found.")
        current_tag = tag_obj
    elif isinstance(some_tag, FilingTag):
        current_tag = some_tag
    else:
        raise TypeError("Tag must be a FilingTag or string label.")
    last_record = None
    while current_tag:
        exists = db_session.query(FileTagLabel).filter_by(
            file_id=file_obj.id,
            tag=current_tag.label
        ).first()
        if exists:
            current_tag = current_tag.parent
            continue
        record = FileTagLabel(
            file_id=file_obj.id,
            file_hash=file_obj.hash,
            tag=current_tag.label,
            is_primary=(current_tag.parent is None),
            label_source=label_source
        )
        db_session.add(record)
        last_record = record
        current_tag = current_tag.parent
    db_session.commit()
    return last_record

def file_tags_from_path(pth: str|Path, session: Session) -> list[FilingTag]:
    """
    Given a filesystem path, return all FilingTag rows whose
    full_tag_label_str appears anywhere in that path.
    """
    path_str = str(pth)
    return (
        session
        .query(FilingTag)
        .filter(
            literal(path_str)
            .ilike(func.concat('%', FilingTag.full_tag_label_str, '%'))
        )
        .all()
    )

def process_files_given_tag(
    filing_code_tag: str,
    file_server_location: str,
    n: int = 250,
    randomize: bool = True,
    exclude_embedded: bool = True,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB,
    text_length_threshold: int = DEFAULT_TEXT_LENGTH_THRESHOLD,
    tesseract_cmd: Optional[str] = None
):
    """Main end-to-end pipeline: extract, embed, and label files for a filing tag.

    Steps:
      1. Configure OCR (Tesseract)
      2. Instantiate MiniLM embedder
      3. Connect to DB and retrieve FilingTag
      4. Query matching files
      5. For each file:
         a. Copy to temp dir
         b. Select extractor or fallback to Tika
         c. Extract, clean, normalize text
         d. Generate embedding
         e. Save embedding & apply tag label
      6. Continue on errors without halting batch

    Parameters:
        filing_code_tag (str): Tag label (e.g., 'F7.1')
        file_server_location (str): Base mount path for file servers
        n (int): Max number of files to process
        randomize (bool): Shuffle file list
        exclude_embedded (bool): Skip files with existing embeddings
        max_size_mb (Optional[float]): Max file size filter
        text_length_threshold (int): Minimum text length to embed
        tesseract_cmd (Optional[str]): Path to Tesseract executable
    """
    # Initialize pipeline logger and OCR configuration
    logger = logging.getLogger('add_files_pipeline')
    init_tesseract(tesseract_cmd)

    # Create embedding client and database session
    embedding_client = MiniLMEmbedder()
    engine = get_db_engine()
    with Session(engine) as session:
        # Retrieve the FilingTag or exit early
        tag = FilingTag.retrieve_tag_by_label(session, filing_code_tag)
        if not tag:
            logger.error(f"Tag '{filing_code_tag}' not found in DB.")
            return

        # Query matching files with filters
        files_query = get_files_from_taggged_locations_query(
            session, tag, n=n,
            randomize=randomize,
            exclude_embedded=exclude_embedded,
            max_size_mb=max_size_mb
        )

        # Use a temporary workspace for file operations
        with tempfile.TemporaryDirectory() as temp_dir:
            files = files_query.all()
            for file_count, file_obj in enumerate(files, start=1):
                logger.info(f"Processing {file_count}/{len(files)}: File ID {file_obj.id}")
                # Skip if no file location
                if not file_obj.locations:
                    logger.warning(f"No locations for file {file_obj.id}")
                    continue

                # Find the correct file location
                local_path = None
                filename = None
                for loc in file_obj.locations:
                    if tag.full_tag_label_str.lower() not in loc.file_server_directories.lower():
                        continue
                    path = loc.local_filepath(server_mount_path=file_server_location)
                    if os.path.exists(path):
                        local_path = path
                        filename = loc.filename
                        break
                if not filename or not local_path:
                    logger.warning(f"File {file_obj.id} not found on server.")
                    continue
                try:
                    # Copy to temp workspace
                    temp_fp = os.path.join(temp_dir, filename)
                    shutil.copyfile(local_path, temp_fp)

                    # Select specialized extractor or fallback
                    extractor = get_extractor_for_file(temp_fp, extractors_list)
                    if extractor:
                        text = extractor(temp_fp)
                    else:
                        text = tika_extractor(temp_fp)

                    # Proceed if text is sufficiently long
                    if text and len(text) >= text_length_threshold:
                        # Clean and normalize text
                        text = common_char_replacements(text)
                        text = strip_diacritics(text)
                        text = normalize_unicode(text)
                        text = normalize_whitespace(text)

                        # Generate embedding
                        emb = embedding_client.encode([text])
                        vec = emb[0] if emb else None
                        if vec is not None:
                            # Persist embedding
                            fe = FileEmbedding(
                                file_hash=file_obj.hash,
                                source_text=text,
                                minilm_model=embedding_client.model_name,
                                minilm_emb=vec
                            )
                            session.add(fe)
                            session.commit()
                            logger.info(f"Embedded file {file_obj.id}")

                            # Label file after embedding
                            label_file_using_tag(session, file_obj, tag)
                        else:
                            logger.warning(f"Embedding failed for {file_obj.id}")
                            continue

                    else:
                        logger.warning(f"Text too short or empty for {file_obj.id}")
                except Exception as exc:
                    # Log and continue to next file
                    logger.error(f"Error {file_obj.id}: {exc}")
                    logger.debug(traceback.format_exc())
                    continue

def process_files_given_file_server_location(
    file_server_location: str,
    n: int = 250,
    exclude_embedded: bool = True,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB,
    text_length_threshold: int = DEFAULT_TEXT_LENGTH_THRESHOLD,
    tesseract_cmd: Optional[str] = None
):
    
    """
    Docstring goes here.
    """
    
    # Initialize pipeline logger and OCR configuration
    logger = logging.getLogger('add_files_pipeline')
    init_tesseract(tesseract_cmd)

    # Create embedding client and database session
    embedding_client = MiniLMEmbedder()
    engine = get_db_engine()
    with Session(engine) as session:
        # query for files in the given file server location
        target_location_dirs = extract_server_dirs(file_server_location)
        files_query = get_files_from_server_locations_query(
            session, target_location_dirs,
            n=n,
            exclude_embedded=exclude_embedded,
            max_size_mb=max_size_mb
        )

        files = files_query.all()
        with tempfile.TemporaryDirectory() as temp_dir:
            for file_count, file_obj in enumerate(files, start=1):
                logger.info(f"Processing {file_count}/{len(files)}: File ID {file_obj.id}")
                # Skip if no file location
                if not file_obj.locations:
                    logger.warning(f"No locations for file {file_obj.id}")
                    continue

                # Find the correct file location
                local_path = None
                filename = None
                for loc in file_obj.locations:
                    if not loc.file_server_directories.startswith(tuple(target_location_dirs)):
                        continue
                    path = loc.local_filepath(server_mount_path=file_server_location)
                    if os.path.exists(path):
                        local_path = path
                        filename = loc.filename
                        break
                
                if not filename or not local_path:
                    logger.warning(f"File {file_obj.id} not found on server.")
                    continue

                try:
                    # determine if the path has tags before doing text extraction and embedding
                    path_tags = file_tags_from_path(local_path, session)
                    if not path_tags:
                        logger.warning(f"No tags found for file {file_obj.id} at {local_path}")
                        continue
                    
                    # Copy to temp workspace
                    temp_fp = os.path.join(temp_dir, filename)
                    shutil.copyfile(local_path, temp_fp)

                    # Select specialized extractor or fallback
                    extractor = get_extractor_for_file(temp_fp, extractors_list)
                    if extractor:
                        text = extractor(temp_fp)
                    else:
                        text = tika_extractor(temp_fp)

                    # Proceed if text is sufficiently long
                    if text and len(text) >= text_length_threshold:
                        # Clean and normalize text
                        text = common_char_replacements(text)
                        text = strip_diacritics(text)
                        text = normalize_unicode(text)
                        text = normalize_whitespace(text)

                        # Generate embedding
                        emb = embedding_client.encode([text])
                        vec = emb[0] if emb else None
                        if vec is not None:
                            # Persist embedding
                            fe = FileEmbedding(
                                file_hash=file_obj.hash,
                                source_text=text,
                                minilm_model=embedding_client.model_name,
                                minilm_emb=vec
                            )
                            session.add(fe)
                            session.commit()
                            logger.info(f"Embedded file {file_obj.id}")

                            # Label file after embedding
                            label_file_using_tag(session, file_obj, FilingTag.retrieve_tag_by_label(session, 'default'))
                        else:
                            logger.warning(f"Embedding failed for {file_obj.id}")
                            continue

                        # Label file using tags
                        for tag in path_tags:
                            label_file_using_tag(session, file_obj, tag)

                    else:
                        logger.warning(f"Text too short or empty for {file_obj.id}")
                except Exception as exc:
                    # Log and continue to next file
                    logger.error(f"Error {file_obj.id}: {exc}")
                    logger.debug(traceback.format_exc())