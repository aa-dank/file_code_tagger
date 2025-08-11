# pipeline/add_files_pipeline.py

import logging
import os
import shutil
import traceback
import tempfile
import pytesseract
from typing import Optional
from db_models import File, FileLocation, FilingTag, FileTagLabel, FileEmbedding, get_db_engine
from embedding.minilm import MiniLMEmbedder
from sqlalchemy import func
from sqlalchemy.orm import Session
from text_extraction.pdf_extraction import PDFTextExtractor
from text_extraction.basic_extraction import TextFileTextExtractor, TikaTextExtractor, get_extractor_for_file
from text_extraction.image_extraction import ImageTextExtractor
from text_extraction.office_doc_extraction import PresentationTextExtractor, SpreadsheetTextExtractor, WordFileTextExtractor
from text_extraction.web_extraction import HtmlTextExtractor, EmailTextExtractor
from text_extraction.extraction_utils import common_char_replacements, strip_diacritics, normalize_unicode, normalize_whitespace

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
    """Set pytesseract command path if provided."""
    if cmd:
        pytesseract.pytesseract.tesseract_cmd = cmd

def get_files_from_tag_locations(
    db_session: Session,
    tag_obj: FilingTag,
    n: int = 100,
    randomize: bool = False,
    exclude_embedded: bool = False,
    max_size_mb: Optional[float] = None
):
    """
    Retrieve files tagged with a specific tag.
    Optionally exclude files that already have an embedding.
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
    return q.all()

def label_file_using_tag(
    db_session: Session,
    file_obj: File,
    some_tag: FilingTag | str,
    label_source: str = 'rule'
):
    """Label a file with a specific tag and its ancestors."""
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


def process_files(
    filing_code_tag: str,
    file_server_location: str,
    n: int = 250,
    randomize: bool = True,
    exclude_embedded: bool = True,
    max_size_mb: Optional[float] = 150,
    text_length_threshold: int = 250,
    tesseract_cmd: Optional[str] = None
):
    """Run the file processing pipeline: extract text, embed, and label."""
    logger = logging.getLogger('add_files_pipeline')
    init_tesseract(tesseract_cmd)
    embedding_client = MiniLMEmbedder()
    engine = get_db_engine()
    with Session(engine) as session:
        tag = FilingTag.retrieve_tag_by_label(session, filing_code_tag)
        if not tag:
            logger.error(f"Tag '{filing_code_tag}' not found in DB.")
            return
        files = get_files_from_tag_locations(
            session, tag, n=n,
            randomize=randomize,
            exclude_embedded=exclude_embedded,
            max_size_mb=max_size_mb
        )
        with tempfile.TemporaryDirectory() as temp_dir:
            for idx, fobj in enumerate(files, start=1):
                logger.info(f"Processing {idx}/{len(files)}: File ID {fobj.id}")
                if not fobj.locations:
                    logger.warning(f"No locations for file {fobj.id}")
                    continue
                local_path = None
                filename = None
                for loc in fobj.locations:
                    if tag.full_tag_label_str.lower() not in loc.file_server_directories.lower():
                        continue
                    path = loc.local_filepath(server_mount_path=file_server_location)
                    if os.path.exists(path):
                        local_path = path
                        filename = loc.filename
                        break
                if not filename or not local_path:
                    logger.warning(f"File {fobj.id} not found on server.")
                    continue
                try:
                    temp_fp = os.path.join(temp_dir, filename)
                    shutil.copyfile(local_path, temp_fp)
                    extractor = get_extractor_for_file(temp_fp, extractors_list)
                    if extractor:
                        text = extractor(temp_fp)
                    else:
                        text = tika_extractor(temp_fp)
                    if text and len(text) >= text_length_threshold:
                        text = common_char_replacements(text)
                        text = strip_diacritics(text)
                        text = normalize_unicode(text)
                        text = normalize_whitespace(text)
                        emb = embedding_client.encode([text])
                        vec = emb[0] if emb else None
                        if vec is not None:
                            fe = FileEmbedding(
                                file_hash=fobj.hash,
                                source_text=text,
                                minilm_model=embedding_client.model_name,
                                minilm_emb=vec
                            )
                            session.add(fe)
                            session.commit()
                            logger.info(f"Embedded file {fobj.id}")
                        else:
                            logger.warning(f"Embedding failed for {fobj.id}")
                            continue
                        label_file_using_tag(session, fobj, tag)
                    else:
                        logger.warning(f"Text too short or empty for {fobj.id}")
                except Exception as exc:
                    logger.error(f"Error {fobj.id}: {exc}")
                    logger.debug(traceback.format_exc())
                    continue
