# pipeline/date_mentions_pipeline.py

import logging
import os
from datetime import date, datetime
from pathlib import Path
from typing import List, Optional, Tuple

from sqlalchemy import func, or_
from sqlalchemy.orm import Session

from db import get_db_engine
from db.models import File, FileLocation, FileContent, FileDateMention
from text_extraction.basic_extraction import DateExtractor
from utils import extract_server_dirs

# Match the logger name with what's configured in the CLI
logger = logging.getLogger('pipeline.date_mentions_pipeline')

def get_files_with_text_in_server_location(
    db_session: Session,
    server_dirs: str | Path,
    n: Optional[int] = None,
    randomize: bool = False,
    ) -> List[Tuple[File, FileContent]]:
    """
    Get files with extracted text in the specified server location (from FileContent table).

    Parameters
    ----------
    db_session : Session
        Active SQLAlchemy session
    server_dirs : str or Path
        The server directory path to search in
    n : Optional[int], default=None
        Maximum number of files to return
    randomize : bool, default=False
        Whether to randomize the order of results

    Returns
    -------
    List[Tuple[File, FileContent]]
        List of (File, FileContent) tuples for files with text in the specified location
    """
    server_dirs_str = str(server_dirs).rstrip('/')
    files_located_in_dir = or_(
        FileLocation.file_server_directories == server_dirs_str,
        FileLocation.file_server_directories.startswith(server_dirs_str + '/')
    )
    
    # Query files in the location that have text in FileContent
    q = db_session.query(File, FileContent)\
        .join(FileLocation, File.id == FileLocation.file_id)\
        .join(FileContent, File.hash == FileContent.file_hash)\
        .filter(files_located_in_dir)\
        .filter(FileContent.source_text.isnot(None))\
        .filter(func.length(FileContent.source_text) > 0)
    
    if randomize:
        q = q.order_by(func.random())
    if n is not None:
        q = q.limit(n)
        
    return q.all()

def extract_and_save_date_mentions(
    db_session: Session,
    file: File,
    content: FileContent,
    date_extractor: DateExtractor
) -> int:
    """
    Extract dates from document text (from FileContent) and save them to the file_date_mentions table.

    Parameters
    ----------
    db_session : Session
        Active SQLAlchemy session
    file : File
        File ORM object
    content : FileContent
        FileContent ORM object with source_text
    
    Returns
    -------
    int
        Number of date mentions extracted and saved
    """
    if not content.source_text:
        logger.warning(f"No text to extract dates from for file {file.hash}")
        return 0

    # Extract dates from text using DateExtractor
    date_hits = date_extractor(content.source_text)
    if not date_hits:
        logger.debug(f"No dates found in text for file {file.hash}")
        return 0

    # Count occurrences of each date
    date_counts = {}
    for mentioned_date in date_hits:
        if mentioned_date not in date_counts:
            date_counts[mentioned_date] = 0
        date_counts[mentioned_date] += 1

    # Save to database
    count = 0
    for mentioned_date, mentions_count in date_counts.items():
        # Check if this date mention already exists for the file
        existing = db_session.query(FileDateMention).filter(
            FileDateMention.file_hash == file.hash,
            FileDateMention.mention_date == mentioned_date
        ).first()

        if existing:
            # Update count if it already exists
            existing.mentions_count = mentions_count
            existing.extracted_at = func.now()
        else:
            # Create new record
            mention = FileDateMention(
                file_hash=file.hash,
                mention_date=mentioned_date,
                mentions_count=mentions_count,
                granularity='day',  # Default - could be enhanced to detect partial dates
                extractor='regex-basic'
            )
            db_session.add(mention)

        count += 1

    # Commit the changes
    if count > 0:
        db_session.commit()
        logger.info(f"Saved {count} date mentions for file {file.hash}")

    return count

def process_date_mentions_for_server_location(
    server_location: str,
    mount: str,
    limit: Optional[int] = None,
    randomize: bool = False
) -> Tuple[int, int]:
    """
    Extract date mentions from files in a specified server location.
    
    Parameters
    ----------
    server_location : str
        The server location path to process
    mount : str
        The base mount path
    limit : Optional[int], default=None
        Maximum number of files to process
    randomize : bool, default=False
        Whether to randomize the order of files
    
    Returns
    -------
    Tuple[int, int]
        (files_processed, date_mentions_found)
    """
    logger.info(f"Processing date mentions for server location: {server_location}")
    
    engine = get_db_engine()
    files_processed = 0
    total_date_mentions = 0
    
    with Session(engine) as session:
        # Extract the server directory path relative to mount
        target_dirs = extract_server_dirs(full_path=server_location, base_mount=mount)
        logger.info(f"Target directories: {target_dirs}")
        
        # Get files with text in the target location
        file_contents = get_files_with_text_in_server_location(
            session, target_dirs, n=limit, randomize=randomize
        )
        
        logger.info(f"Found {len(file_contents)} files with extracted text")
        dt_extractor = DateExtractor()
        # Process each file
        for file, content in file_contents:
            try:
                date_count = extract_and_save_date_mentions(db_session=session,
                                                            file=file,
                                                            content=content,
                                                            date_extractor=dt_extractor)
                total_date_mentions += date_count
                files_processed += 1
                
                if files_processed % 100 == 0:
                    logger.info(f"Processed {files_processed}/{len(file_contents)} files")
                    
            except Exception as e:
                logger.error(f"Error processing file {file.hash}: {str(e)}")
                continue
    
    logger.info(f"Completed processing {files_processed} files, found {total_date_mentions} date mentions")
    return files_processed, total_date_mentions
