import hashlib
import logging
import numpy as np
from numpy.linalg import norm
from pathlib import Path, PurePosixPath

logger = logging.getLogger(__name__)

def extract_server_dirs(full_path: str | Path,
                       base_mount: str | Path,
                       include_filename: bool = False) -> str:
    """
    Extract the server directory path (and optionally filename) relative to a mount point.

    Parameters
    ----------
    full_path   Absolute path on the client machine
                e.g. r"N:\\PPDO\\Records\\49xx   Long Marine Lab\\4932\\file.pdf"
    base_mount  The local mount-point for the records share
                e.g. r"N:\\PPDO\\Records"   or   "/mnt/records"
    include_filename : bool, default False
                Whether to include the filename in the returned path.
                If False, only returns the directory structure.

    Returns
    -------
    str   --  value suitable for file_locations.file_server_directories
              (always forward-slash separators, no leading slash)
              If include_filename=False, excludes the filename component.
    """
    # Normalise to platform-aware Path objects
    full = Path(full_path).expanduser().resolve()
    base = Path(base_mount).expanduser().resolve()

    # 1) Get the sub-path *relative* to the mount
    try:
        rel_parts = full.relative_to(base)
    except ValueError:               # not under base_mount
        raise ValueError(f"{full} is not under {base}")

    # 2) If include_filename is False, exclude the filename (last part)
    if not include_filename and rel_parts.parts:
        # Remove the last part (filename) if it exists
        rel_parts = Path(*rel_parts.parts[:-1]) if len(rel_parts.parts) > 1 else Path()

    # 3) Convert to POSIX form (forces forward slashes)
    return str(PurePosixPath(rel_parts))

def build_file_path(base_mount: str,
                    server_dir: str,
                    filename: str = None) -> Path:
    """
    Join a server-relative path + filename onto a machine-specific
    mount-point.

    Parameters
    ----------
    base_mount : str
        The local mount of the records share, e.g.
        r"N:\PPDO\Records"  (Windows)  or  "/mnt/records" (Linux).
    server_dir : str
        The value from file_locations.file_server_directories
        (always stored with forward-slashes).
    filename   : str
        file_locations.filename

    Returns
    -------
    pathlib.Path  – ready for open(), exists(), etc.
    """
    # 1) Treat the DB field as a *POSIX* path (it always uses “/”)
    rel_parts = PurePosixPath(server_dir).parts     # -> tuple of segments

    # 2) Let Path figure out the separator style of this machine
    full_path = Path(base_mount).joinpath(*rel_parts)
    if filename:
        full_path = full_path / filename
    
    return full_path

def file_tag_prefix(file_tag:str) -> str:
    """
    Returns the prefix for a file tag.
    """
    return file_tag.split(" ")[0] + " - "

def get_hash(filepath, hash_algo=hashlib.sha1):
    """"
    This function takes a filepath and a hash algorithm as input and returns the hash of the file at the filepath
    """
    def chunk_reader(fobj, chunk_size=1024):
        """ Generator that reads a file in chunks of bytes """
        while True:
            chunk = fobj.read(chunk_size)
            if not chunk:
                return
            yield chunk

    hashobj = hash_algo()
    with open(filepath, "rb") as f:
        for chunk in chunk_reader(f):
            hashobj.update(chunk)

    return hashobj.hexdigest()

def bytes_in_mb(bytes_size: int) -> float:
    """
    Convert bytes to megabytes.
    
    Parameters
    ----------
    bytes_size : int
        Size in bytes.
    
    Returns
    -------
    float
        Size in megabytes.
    """
    return bytes_size / (1024 * 1024) if bytes_size else 0.0

def cosine_similarity(a, b):
    """
    Compute cosine similarity between two vectors safely.
    Vectorized when possible, but avoids div/0 errors.
    """
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)
    na = np.linalg.norm(a)
    nb = np.linalg.norm(b)
    if na == 0.0 or nb == 0.0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))

def cosine_similarity_batch(query_vec, matrix):
    """
    Compute cosine similarity between a single vector and multiple vectors.
    
    Parameters
    ----------
    query_vec : array-like, shape (d,)
        Single query vector
    matrix : array-like, shape (n, d) 
        Matrix where each row is a vector to compare against
        
    Returns
    -------
    np.ndarray, shape (n,)
        Cosine similarities between query_vec and each row of matrix
    """
    query_vec = np.asarray(query_vec, dtype=float)
    matrix = np.asarray(matrix, dtype=float)
    
    # Normalize query vector
    query_norm = np.linalg.norm(query_vec)
    if query_norm == 0.0:
        return np.zeros(matrix.shape[0])
    
    # Normalize matrix rows
    matrix_norms = np.linalg.norm(matrix, axis=1)
    zero_mask = matrix_norms == 0.0
    
    # Compute dot products
    dots = np.dot(matrix, query_vec)
    
    # Compute cosine similarities, handling zero norms
    sims = np.zeros(matrix.shape[0])
    valid_mask = ~zero_mask
    sims[valid_mask] = dots[valid_mask] / (matrix_norms[valid_mask] * query_norm)
    
    return sims