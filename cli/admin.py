"""Administrative CLI commands (database maintenance, etc.).

Usage examples:

  python -m cli.admin backup-db 
  python -m cli.admin backup-db --backup-dir dev --compress

Environment variables required for DB connection (loaded via .env):
  PROJECT_DB_USERNAME, PROJECT_DB_PASSWORD, PROJECT_DB_HOST,
  PROJECT_DB_PORT, PROJECT_DB_NAME
"""

# Load environment variables from .env file early
from dotenv import load_dotenv
load_dotenv()

import os
import logging
import click

from logging_setups import setup_logger
from db.db import backup_database


# Root group (allows future expansion: restore-db, vacuum, etc.)
@click.group()
def cli():
	"""Admin / maintenance commands."""
	pass


@cli.command('backup-db')
@click.option('--backup-dir', '-d', default='dev', show_default=True,
			  help='Directory to store backup file (created if missing).')
@click.option('--compress/--no-compress', default=False, show_default=True,
			  help='Compress backup using bz2 (streamed).')
@click.option('--log-file', default='app.log', show_default=True, help='Log file path.')
@click.option('--log-level', default='INFO', show_default=True,
			  type=click.Choice(['DEBUG','INFO','WARNING','ERROR','CRITICAL'], case_sensitive=False))
def backup_db_cmd(backup_dir, compress, log_file, log_level):
	"""Create a timestamped PostgreSQL database backup.

	The file will be named like archives_backup_YYYYMMDD_HHMMSS.sql[.bz2]
	and placed in the chosen directory.
	"""
	logger = setup_logger(
		name='admin.backup',
		log_file=log_file,
		level=getattr(logging, log_level),
		console=True
	)

	logger.info("Starting database backup")

	# Basic validation of required env vars (fail fast for clearer UX)
	required_env = [
		'PROJECT_DB_USERNAME', 'PROJECT_DB_PASSWORD', 'PROJECT_DB_HOST',
		'PROJECT_DB_PORT', 'PROJECT_DB_NAME'
	]
	missing = [v for v in required_env if not os.getenv(v)]
	if missing:
		logger.error(f"Missing required environment variables: {', '.join(missing)}")
		raise click.Abort()

	try:
		path = backup_database(backup_dir=backup_dir, compress=compress)
		logger.info(f"Backup complete: {path}")
		click.echo(path)  # stdout so script usage can capture
	except FileNotFoundError as e:
		# Common if pg_dump not installed / not on PATH
		logger.exception("pg_dump not found. Ensure PostgreSQL client tools are installed and on PATH.")
		raise click.ClickException(str(e))
	except Exception as e:  # noqa: BLE001 - broad to report unexpected issues cleanly for CLI
		logger.exception("Backup failed")
		raise click.ClickException(str(e))


if __name__ == '__main__':
	cli()

