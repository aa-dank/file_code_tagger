# cli/extract_date_mentions.py

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

import click
import logging
import os
from pathlib import Path

from pipeline.date_mentions_pipeline import process_date_mentions_for_server_location
from logging_setups import setup_logger

@click.command()
@click.option('--path', '-p', required=True, help='Server path to extract date mentions from')
@click.option('--mount', '-m', required=True, help='Base mount path for the file server')
@click.option('--limit', '-n', type=int, default=None, help='Maximum number of files to process')
@click.option('--random/--no-random', default=False, show_default=True, help='Process files in random order')
@click.option('--log-file', default='app.log', show_default=True, help='Log file path')
@click.option('--log-level', default='INFO', show_default=True, 
              type=click.Choice(['DEBUG','INFO','WARNING','ERROR','CRITICAL'], case_sensitive=False))
def extract_dates(path, mount, limit, random, log_file, log_level):
    """
    CLI tool to extract date mentions from documents in a specified server path.

    Example command:
        python -m cli.extract_date_mentions --path "N:\\PPDO\\Records\\63xx   Music Facility\\6301" --mount "N:\\PPDO\\Records" --limit 100
    """
    # Setup logger
    cli_logger = setup_logger(
        name='extract_dates_cli', 
        log_file=log_file, 
        level=getattr(logging, log_level), 
        console=True
    )
    
    # Also wire up the pipeline logger - match the name used in pipeline module
    setup_logger(
        name='pipeline.date_mentions_pipeline', 
        log_file=log_file, 
        level=getattr(logging, log_level), 
        console=True
    )
    
    cli_logger.info(f"Starting date mention extraction for path: {path}")
    
    # Validate paths
    if not os.path.exists(mount):
        cli_logger.error(f"Mount path does not exist: {mount}")
        return 1
    
    full_path = Path(path)
    if not os.path.exists(full_path):
        cli_logger.error(f"Server path does not exist: {path}")
        return 1
    
    # Process date mentions
    try:
        files_processed, mentions_found = process_date_mentions_for_server_location(
            server_location=path,
            mount=mount,
            limit=limit,
            randomize=random
        )
        
        cli_logger.info(f"Successfully processed {files_processed} files")
        cli_logger.info(f"Found {mentions_found} date mentions")
        return 0
    
    except Exception as e:
        cli_logger.exception(f"Error processing date mentions: {str(e)}")
        return 1

if __name__ == "__main__":
    extract_dates()
