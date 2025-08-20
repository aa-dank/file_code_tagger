# test_date_extraction.py

import logging
from dotenv import load_dotenv
from sqlalchemy.orm import Session

from db import get_db_engine
from db.models import FileDateMention, File, FileEmbedding
from pipeline.date_mentions_pipeline import extract_and_save_date_mentions

# Load environment variables
load_dotenv()

# Set up logging
logging.basicConfig(level=logging.INFO, 
                   format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_date_extraction():
    """
    Test the date extraction on a sample file with known text.
    """
    engine = get_db_engine()
    
    with Session(engine) as session:
        # Find a file with text content
        file_with_text = session.query(File, FileEmbedding)\
            .join(FileEmbedding, File.hash == FileEmbedding.file_hash)\
            .filter(FileEmbedding.source_text.isnot(None))\
            .first()
            
        if not file_with_text:
            logger.error("No files with text found in database")
            return
            
        file, embedding = file_with_text
        
        # Delete any existing date mentions for this file to start fresh
        session.query(FileDateMention)\
            .filter(FileDateMention.file_hash == file.hash)\
            .delete()
        session.commit()
        
        # Extract and save date mentions
        logger.info(f"Testing date extraction for file hash: {file.hash}")
        date_count = extract_and_save_date_mentions(session, file, embedding)
        
        # Verify results
        if date_count > 0:
            logger.info(f"Successfully extracted {date_count} date mentions")
            
            # Show the extracted dates
            date_mentions = session.query(FileDateMention)\
                .filter(FileDateMention.file_hash == file.hash)\
                .all()
                
            logger.info("Extracted dates:")
            for mention in date_mentions:
                logger.info(f"  {mention.mention_date} (mentioned {mention.mentions_count} times)")
        else:
            logger.info("No dates found in the text")

if __name__ == "__main__":
    test_date_extraction()
