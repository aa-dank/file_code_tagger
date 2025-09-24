# pipeline/add_files_pipeline.py

import logging
import os
import shutil
import tempfile
import traceback
import pytesseract
from pathlib import Path
from typing import Optional
from db.models import File, FileLocation, FilingTag, FileTagLabel, FileContent
from db import get_db_engine
from embedding.minilm import MiniLMEmbedder
from sqlalchemy import func, or_
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

def get_files_from_tagged_locations_query(
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
        q = q.outerjoin(FileContent, File.hash == FileContent.file_hash)\
             .filter(FileContent.file_hash == None)
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
    server_dirs_str = str(server_dirs).rstrip('/')
    files_located_in_dir = or_(
        FileLocation.file_server_directories == server_dirs_str,
        FileLocation.file_server_directories.startswith(server_dirs_str + '/')
    )
    q = db_session.query(File)\
        .join(FileLocation)\
        .filter(files_located_in_dir)
    if exclude_embedded:
        q = q.outerjoin(FileContent, File.hash == FileContent.file_hash)\
             .filter(FileContent.file_hash == None)
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
    path_str = str(pth).lower()
    all_tags = session.query(FilingTag).all()
    return [tag for tag in all_tags if tag.full_tag_label_str.lower() in path_str]

# --- helper functions to DRY up file processing loops ---
def _locate_for_tag(file_obj, server_mount, tag):
    """
    Locate the first FileLocation for a given File that matches a FilingTag.

    Parameters
    ----------
    file_obj : File
        ORM File instance containing .locations.
    server_mount : str
        Base mount path on the local machine.
    tag : FilingTag
        The FilingTag whose full_tag_label_str is used to filter locations.

    Returns
    -------
    tuple
        (local_path, filename, tag) where:
          local_path : str
            Full filesystem path to the file on the local machine, or None.
          filename : str
            The filename component, or None.
          tag : FilingTag
            The original tag passed in, or None if not found.
    """
    for loc in file_obj.locations:
        if tag.full_tag_label_str.lower() not in loc.file_server_directories.lower():
            continue
        path = loc.local_filepath(server_mount)
        if path and os.path.exists(path):
            return path, loc.filename, tag
    return None, None, None

def _locate_for_location(session, file_obj, server_mount, target_dirs):
    """
    Locate the first FileLocation for a File under specified server directories 
    and retrieve any FilingTags inferred from the path.

    Parameters
    ----------
    session : Session
        Active SQLAlchemy session for querying tags.
    file_obj : File
        ORM File instance containing .locations.
    server_mount : str
        Base mount path on the local machine.
    target_dirs : str
        POSIX-style path fragment to match FileLocation.file_server_directories.

    Returns
    -------
    tuple
        (local_path, filename, tags) where:
          local_path : str
            Full filesystem path to the file, or None.
          filename : str
            The filename component, or None.
          tags : list[FilingTag]
            List of tags whose full_tag_label_str appears in the path; empty if none.
    """
    for loc in file_obj.locations:
        if not loc.file_server_directories.startswith(target_dirs):
            continue
        path = loc.local_filepath(server_mount)
        if path and os.path.exists(path):
            path_tags = file_tags_from_path(path, session)
            return path, loc.filename, path_tags
    return None, None, None

def _label_for_tag(session, file_obj, tag):
    """
    Apply a single FilingTag (and its ancestors) to a File record.

    Parameters
    ----------
    session : Session
        Active SQLAlchemy session.
    file_obj : File
        ORM File instance to label.
    tag : FilingTag
        The FilingTag to apply.
    """
    label_file_using_tag(session, file_obj, tag)

def _label_for_location(session, file_obj, tags):
    """
    Apply default and inferred FilingTags to a File based on its server path.

    Parameters
    ----------
    session : Session
        Active SQLAlchemy session.
    file_obj : File
        ORM File instance to label.
    tags : list[FilingTag]
        Inferred tags from the file path; each will be applied after the default tag.
    """
    for t in tags:
        label_file_using_tag(session, file_obj, t)

def _run_file_pipeline(
    files,
    server_mount,
    session,
    embedding_client,
    tesseract_cmd,
    text_length_threshold,
    locator_fn,
    labeling_fn,
    apply_exclusions: bool = True
):
    """
    Core loop to process, extract, embed, and label a list of File ORM objects.

    Parameters
    ----------
    files : list[File]
        List of ORM File instances to process.
    server_mount : str
        Base mount path for locating files.
    session : Session
        Active SQLAlchemy session for database operations.
    embedding_client : MiniLMEmbedder
        Client for generating vector embeddings.
    tesseract_cmd : Optional[str]
        Path to tesseract executable for OCR; passed to init_tesseract.
    text_length_threshold : int
        Minimum length of extracted text required to proceed with embedding.
    locator_fn : callable
        Function to locate the file on disk and return (path, filename, extra).
    labeling_fn : callable
        Function to apply labels to a File after successful embedding.

    Notes
    -----
    - Copies files to a temporary directory before extraction.
    - Uses a specialized extractor or Tika fallback for text extraction.
    - Normalizes and cleans text before embedding.
    - Commits each embedding and labeling operation immediately.
    """
    # Lazy import to avoid circular imports
    try:
        from db.models import PathPattern  # type: ignore
    except Exception:
        PathPattern = None  # Fallback if model not available

    logger = logging.getLogger('add_files_pipeline')
    init_tesseract(tesseract_cmd)
    for idx, file_obj in enumerate(files, start=1):
        logger.info(f"Processing {idx}/{len(files)}: File hash {file_obj.hash}")
        if not file_obj.locations:
            logger.warning(f"No locations for file hash {file_obj.hash}. Skipping.")
            continue
        local_path, filename, extra = locator_fn(session, file_obj, server_mount)
        if not local_path or not filename:
            logger.warning(f"File hash {file_obj.hash} not found on server using the locator function.")
            continue
        # Exclude from embedding based on dedicated context
        if apply_exclusions and PathPattern is not None:
            try:
                if PathPattern.is_excluded(session, str(local_path), context='add_files_embedding'):
                    logger.info(f"Skipping embedding for excluded file: {local_path}")
                    continue
            except Exception as _exc:
                logger.warning(f"PathPattern exclusion check failed for {local_path}: {_exc}")

        with tempfile.TemporaryDirectory() as temp_dir:
            try:
                temp_fp = os.path.join(temp_dir, filename)
                shutil.copyfile(local_path, temp_fp)
                extractor = get_extractor_for_file(temp_fp, extractors_list)
                text = extractor(temp_fp) if extractor else tika_extractor(temp_fp)
                if text and len(text) >= text_length_threshold:
                    text = common_char_replacements(text)
                    text = strip_diacritics(text)
                    text = normalize_unicode(text)
                    text = normalize_whitespace(text)
                    emb = embedding_client.encode([text])
                    vec = emb[0] if emb else None
                    if vec is not None:
                        fc = FileContent(
                            file_hash=file_obj.hash,
                            source_text=text,
                            text_length=len(text),
                            minilm_model=embedding_client.model_name,
                            minilm_emb=vec
                        )
                        session.add(fc)
                        session.commit()
                        logger.info(f"Embedded file {file_obj.hash} with {embedding_client.model_name}")
                        # Apply tagging only if not excluded for tagging context
                        if not (apply_exclusions and PathPattern is not None and
                                PathPattern.is_excluded(session, str(local_path), context='add_files_tagging')):
                            labeling_fn(session, file_obj, extra)
                        else:
                            logger.info(f"Skipping tagging for excluded file: {local_path}")
                    else:
                        logger.warning(f"Embedding failed for {file_obj.hash}")
                else:
                    logger.warning(f"Text too short or empty for {file_obj.hash}")
            except Exception as exc:
                logger.error(f"Error {file_obj.hash}: {exc}")
                logger.debug(traceback.format_exc())

def process_files_given_tag(
    filing_code_tag: str,
    file_server_location: str,
    n: int = 250,
    randomize: bool = True,
    exclude_embedded: bool = True,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB,
    text_length_threshold: int = DEFAULT_TEXT_LENGTH_THRESHOLD,
    tesseract_cmd: Optional[str] = None,
    apply_exclusions: bool = True
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
    engine = get_db_engine()
    with Session(engine) as session:
        tag = FilingTag.retrieve_tag_by_label(session, filing_code_tag)
        if not tag:
            logging.getLogger('add_files_pipeline').error(
                f"Tag '{filing_code_tag}' not found in DB.")
            return
        files = get_files_from_tagged_locations_query(
            session, tag, n=n, randomize=randomize,
            exclude_embedded=exclude_embedded, max_size_mb=max_size_mb
        ).all()
        _run_file_pipeline(
            files,
            file_server_location,
            session,
            MiniLMEmbedder(),
            tesseract_cmd,
            text_length_threshold,
            locator_fn=lambda _s, f, m: _locate_for_tag(f, m, tag),
            labeling_fn=_label_for_tag,
            apply_exclusions=apply_exclusions
        )

def process_files_given_file_server_location(
    file_server_location: str,
    mount: str,
    n: int = 250,
    exclude_embedded: bool = True,
    max_size_mb: Optional[float] = DEFAULT_MAX_SIZE_MB,
    text_length_threshold: int = DEFAULT_TEXT_LENGTH_THRESHOLD,
    tesseract_cmd: Optional[str] = None,
    apply_exclusions: bool = True
):
    """
    Main pipeline: extract, embed, and label files based on a server location.

    Steps:
      1. Configure OCR (Tesseract) with optional custom command
      2. Instantiate MiniLM embedder
      3. Connect to DB and query files under the given server dirs
      4. For each file:
         a. Copy to a temporary workspace
         b. Select a specialized extractor or fallback to Tika
         c. Extract text, then clean and normalize it
         d. Generate an embedding vector
         e. Save the embedding and apply default + inferred tags
      5. Continue processing even if individual files error

    Parameters:
        file_server_location (str): Base mount path for file storage.
        n (int): Maximum number of files to process.
        exclude_embedded (bool): If True, skip files with existing embeddings.
        max_size_mb (Optional[float]): Maximum file size (in MB) to include.
        text_length_threshold (int): Minimum character length for extracted text.
        tesseract_cmd (Optional[str]): Path to tesseract executable for OCR.
    """
    engine = get_db_engine()
    with Session(engine) as session:
        target_dirs = extract_server_dirs(full_path=file_server_location,
                                          base_mount=mount)
        files = get_files_from_server_locations_query(
            session, target_dirs, n=n,
            exclude_embedded=exclude_embedded, max_size_mb=max_size_mb
        ).all()
        _run_file_pipeline(
            files=files,
            server_mount=mount,
            session=session,
            embedding_client=MiniLMEmbedder(),
            tesseract_cmd=tesseract_cmd,
            text_length_threshold=text_length_threshold,
            locator_fn=lambda _s, f, m: _locate_for_location(_s, f, m, target_dirs),
            labeling_fn=_label_for_location,
            apply_exclusions=apply_exclusions
        )