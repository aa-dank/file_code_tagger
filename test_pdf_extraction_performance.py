"""
Performance comparison script for PDF text extraction methods.

This script benchmarks PDFTextExtractor and PDFTextExtractor2 on a set of PDF files.
It can reuse a persistent file collection in the database for repeatable tests, or create a new one.

Usage:
	- Set environment variable PDF_PERF_TEST_N to control the number of PDFs (default: 10).
	- Set PDF_SERVER_MOUNT if file paths require a mount prefix.
	- The script will use a collection named 'perf_test_pdf_extract' if it exists, otherwise it will create one.

Tables used: files, file_locations, file_collections, file_collection_members
"""

import dotenv
import os
import time
from datetime import datetime
from pathlib import Path
from text_extraction.pdf_extraction import PDFTextExtractor, PDFTextExtractor2, PDFFile
import csv
import statistics

# --- DB imports ---
from db import get_db_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func
from db.models import File, FileLocation, FileCollection, FileCollectionMember, Base

dotenv.load_dotenv()

def get_random_pdf_files_from_db(session, n=10):
	"""
	Query for n random PDF files with at least one valid location.

	Args:
		session: SQLAlchemy session object.
		n (int): Number of PDF files to return.

	Returns:
		list[File]: List of File ORM objects with .pdf extension and at least one location.
	"""
	pdf_files = session.query(File).join(FileLocation).filter(File.extension == 'pdf')\
		.order_by(func.random())\
		.limit(n)\
		.all()
	if not pdf_files:
		return []
	return list(pdf_files)

def save_file_collection(session, files, name=None, description=None, role='test'):
	"""
	Save a list of File objects as a new FileCollection in the database.

	Args:
		session: SQLAlchemy session object.
		files (list[File]): List of File ORM objects to add to the collection.
		name (str, optional): Name for the collection. If None, a timestamped name is used.
		description (str, optional): Description for the collection.
		role (str): Role label for each file in the collection (default: 'test').

	Returns:
		FileCollection: The created FileCollection ORM object.
	"""
	if not name:
		name = f"pdf_extraction_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
	collection = FileCollection(name=name, description=description or "Random PDF files for extraction performance test")
	session.add(collection)
	session.flush()  # get collection.id
	for f in files:
		session.add(FileCollectionMember(collection_id=collection.id, file_id=f.id, role=role))
	session.commit()
	return collection

def get_local_filepaths(files: list[File], server_mount=None):
	"""
	Get local file paths for a list of File objects using their first location.

	Args:
		files (list[File]): List of File ORM objects.
		server_mount (str, optional): Base path to prepend for local file resolution.

	Returns:
		list[str]: List of resolved file paths as strings.
	"""
	paths = []
	if not server_mount:
		raise ValueError("server_mount must be provided to resolve local file paths")
	for f in files:
		if f.locations:
			for loc in f.locations:
				p = loc.local_filepath(server_mount)
				if os.path.exists(p) and os.path.isfile(p):
					paths.append(str(p))
					break
	return paths

def compare_extractors(pdf_files, csv_path=None):
	"""
	Compare the performance and output of PDFTextExtractor and PDFTextExtractor2.

	Args:
		pdf_files (list[str]): List of file paths to PDF files.
		csv_path (str, optional): Path to save CSV results.

	Returns:
		list[dict]: List of result dicts with timing, output length, and errors for each extractor.
	"""
	extractor1 = PDFTextExtractor()
	extractor2 = PDFTextExtractor2()
	results = []
	for pdf in pdf_files:
		print(f"\nTesting: {pdf}")
		# Get file size and page count
		try:
			pdf_file = PDFFile(pdf)
			size_mb = pdf_file.size / (1024 * 1024)
			page_count = pdf_file.page_count
		except Exception as e:
			size_mb = None
			page_count = None
		# PDFTextExtractor
		t0 = time.time()
		try:
			text1 = extractor1(pdf)
			err1 = None
		except Exception as e:
			text1 = ''
			err1 = str(e)
		t1 = time.time()
		# PDFTextExtractor2
		t2 = time.time()
		try:
			text2 = extractor2(pdf)
			err2 = None
		except Exception as e:
			text2 = ''
			err2 = str(e)
		t3 = time.time()
		results.append({
			'file': pdf,
			'mb_size': size_mb,
			'page_count': page_count,
			'extractor1_time': t1-t0,
			'extractor2_time': t3-t2,
			'extractor1_len': len(text1),
			'extractor2_len': len(text2),
			'extractor1_err': err1,
			'extractor2_err': err2
		})
		print(f"  PDFTextExtractor:   {t1-t0:.2f}s, length={len(text1)}, error={err1}")
		print(f"  PDFTextExtractor2:  {t3-t2:.2f}s, length={len(text2)}, error={err2}")

	# Write CSV if requested
	if csv_path:
		with open(csv_path, "w", newline='', encoding="utf-8") as f:
			writer = csv.DictWriter(f, fieldnames=[
				'file', 'mb_size', 'page_count',
				'extractor1_time', 'extractor2_time',
				'extractor1_len', 'extractor2_len',
				'extractor1_err', 'extractor2_err'
			])
			writer.writeheader()
			for row in results:
				writer.writerow(row)
	return results

def print_summary_stats(results):
	"""
	Print summary statistics comparing the two extractors' performance.
	"""
	def safe_mean(vals):
		vals = [v for v in vals if v is not None]
		return statistics.mean(vals) if vals else float('nan')
	def safe_median(vals):
		vals = [v for v in vals if v is not None]
		return statistics.median(vals) if vals else float('nan')
	def safe_stdev(vals):
		vals = [v for v in vals if v is not None]
		return statistics.stdev(vals) if len(vals) > 1 else float('nan')

	times1 = [r['extractor1_time'] for r in results]
	times2 = [r['extractor2_time'] for r in results]
	lens1 = [r['extractor1_len'] for r in results]
	lens2 = [r['extractor2_len'] for r in results]
	errs1 = [r['extractor1_err'] for r in results]
	errs2 = [r['extractor2_err'] for r in results]

	print("\nSummary statistics:")
	print(f"  PDFTextExtractor:   mean={safe_mean(times1):.2f}s, median={safe_median(times1):.2f}s, stdev={safe_stdev(times1):.2f}s, errors={sum(1 for e in errs1 if e)}")
	print(f"  PDFTextExtractor2:  mean={safe_mean(times2):.2f}s, median={safe_median(times2):.2f}s, stdev={safe_stdev(times2):.2f}s, errors={sum(1 for e in errs2 if e)}")
	print(f"  Output length mean: extractor1={safe_mean(lens1):.0f}, extractor2={safe_mean(lens2):.0f}")

if __name__ == "__main__":
	"""
	Main entry point for the PDF extraction performance test.

	- Connects to the database.
	- Uses or creates a persistent file collection for repeatable benchmarking.
	- Resolves file paths and runs both extractors on each file.
	- Prints timing and output statistics for each extractor.
	"""
	# Connect to DB
	engine = get_db_engine()
	Session = sessionmaker(bind=engine)
	session = Session()

	# Try to find existing collection first
	collection_name = "perf_test_pdf_extract"
	collection = session.query(FileCollection).filter_by(name=collection_name).first()
	if collection:
		print(f"Using existing collection: {collection.name} (id={collection.id})")
		# Get files from collection
		pdf_file_objs = [m.file for m in collection.members]
	else:
		# Query for random PDF files
		n = int(os.environ.get("PDF_PERF_TEST_N", 100))
		pdf_file_objs = get_random_pdf_files_from_db(session, n=n)
		if not pdf_file_objs:
			print("No PDF files with locations and .pdf extension found in DB.")
			exit(1)
		# Save the collection
		collection = save_file_collection(session, pdf_file_objs, name=collection_name)
		print(f"Saved {len(pdf_file_objs)} files to collection: {collection.name} (id={collection.id})")

	# Get local filepaths (assume server_mount is not needed or set via env)
	server_mount = os.environ.get("FILE_SERVER_MOUNT")
	pdf_paths = get_local_filepaths(pdf_file_objs, server_mount=server_mount)
	if not pdf_paths:
		print("No valid file paths found for selected PDFs.")
		exit(1)

	# Save results to CSV in current directory
	csv_path = "pdf_extraction_performance_results.csv"
	results = compare_extractors(pdf_paths, csv_path=csv_path)
	print(f"\nResults saved to {csv_path}")

	print("\nSummary:")
	for r in results:
		print(f"{os.path.basename(r['file'])}: PDFTextExtractor={r['extractor1_time']:.2f}s/{r['extractor1_len']} chars, "
			  f"PDFTextExtractor2={r['extractor2_time']:.2f}s/{r['extractor2_len']} chars")
	print_summary_stats(results)
