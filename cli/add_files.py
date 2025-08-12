# cli/add_files.py

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

import click
import logging
from pipeline.add_files_pipeline import (
    process_files_given_tag,
    process_files_given_file_server_location
)
from logging_setups import setup_logger

# --- convert to a group so we can add multiple commands ---
@click.group()
def cli():
    """CLI group for file processing commands."""
    pass

@cli.command('by-tag')
@click.option('--tag', '-t', required=True, help='Filing tag label to process')
@click.option('--mount', '-m', required=True, help='File server mount path, e.g., N:\\PPDO\\Records')
@click.option('--number', '-n', default=250, show_default=True, help='Number of files to process')
@click.option('--randomize/--no-randomize', default=True, show_default=True)
@click.option('--exclude-embedded/--include-embedded', default=True, show_default=True)
@click.option('--max-size-mb', default=150, show_default=True, type=float)
@click.option('--threshold', default=250, show_default=True, help='Minimum text length to embed')
@click.option('--tesseract-cmd', default=None, help='Path to tesseract executable')
@click.option('--log-file', default='app.log', show_default=True, help='Log file path')
@click.option('--log-level', default='INFO', show_default=True, type=click.Choice(['DEBUG','INFO','WARNING','ERROR','CRITICAL'], case_sensitive=False))
def add_tag_files(
    tag, mount, number, randomize, exclude_embedded, max_size_mb, threshold, tesseract_cmd, log_file, log_level
):
    """
    CLI tool to process and embed files for a given filing tag

    Example command:
        python -m cli.add_files by-tag --tag "D4 - Mitigation Monitoring Program" --mount "N:\\PPDO\\Records" --number 250 \
--randomize --exclude-embedded --max-size-mb 150 --threshold 250 \
--tesseract-cmd "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
    """
    # Setup logger
    cli_logger = setup_logger(name='add_files_cli', log_file=log_file, level=getattr(logging, log_level), console=True)
    # also wire up your pipeline logger
    setup_logger(
      name='add_files_pipeline',
      log_file=log_file,
      level=getattr(logging, log_level),
      console=True
    )

    cli_logger.info(f"Starting add_files for tag={tag}")
    # Run pipeline
    process_files_given_tag(
        filing_code_tag=tag,
        file_server_location=mount,
        n=number,
        randomize=randomize,
        exclude_embedded=exclude_embedded,
        max_size_mb=max_size_mb,
        text_length_threshold=threshold,
        tesseract_cmd=tesseract_cmd
    )
    cli_logger.info("Completed add_files processing.")

@cli.command('by-location')
@click.option('--mount', '-m', required=True, help='File server mount path, e.g., N:\\PPDO\\Records')
@click.option('--number', '-n', default=250, show_default=True, help='Number of files to process')
@click.option('--exclude-embedded/--include-embedded', default=True, show_default=True)
@click.option('--max-size-mb', default=150, show_default=True, type=float)
@click.option('--threshold', default=250, show_default=True, help='Minimum text length to embed')
@click.option('--tesseract-cmd', default=None, help='Path to tesseract executable')
@click.option('--log-file', default='app.log', show_default=True, help='Log file path')
@click.option('--log-level', default='INFO', show_default=True, type=click.Choice(['DEBUG','INFO','WARNING','ERROR','CRITICAL'], case_sensitive=False))
def add_location_files(
    mount, number, exclude_embedded, max_size_mb, threshold, tesseract_cmd, log_file, log_level
):
    """
    CLI tool to process and embed files for a given server location

    Example command:
        python -m cli.add_files by-location --mount "N:\\PPDO\\Records" --number 200 \
--exclude-embedded --max-size-mb 100 --threshold 300 \
--tesseract-cmd "C:\\Program Files\\Tesseract-OCR\\tesseract.exe"
    """
    # Setup logger(s)
    cli_logger = setup_logger(
        name='add_files_cli',
        log_file=log_file,
        level=getattr(logging, log_level),
        console=True
    )
    setup_logger(
        name='add_files_pipeline',
        log_file=log_file,
        level=getattr(logging, log_level),
        console=True
    )

    cli_logger.info(f"Starting add_files for mount={mount}")
    process_files_given_file_server_location(
        file_server_location=mount,
        n=number,
        exclude_embedded=exclude_embedded,
        max_size_mb=max_size_mb,
        text_length_threshold=threshold,
        tesseract_cmd=tesseract_cmd
    )
    cli_logger.info("Completed add_files processing.")

if __name__ == '__main__':
    cli()
