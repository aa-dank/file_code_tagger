import hashlib
from pathlib import Path, PurePosixPath

def extract_server_dir(full_path: str | Path,
                       base_mount: str | Path) -> str:
    """
    Parameters
    ----------
    full_path   Absolute path on the client machine
                e.g. r"N:\\PPDO\\Records\\49xx   Long Marine Lab\\4932\\..."
    base_mount  The local mount-point for the records share
                e.g. r"N:\\PPDO\\Records"   or   "/mnt/records"

    Returns
    -------
    str   --  value suitable for file_locations.file_server_directories
              (always forward-slash separators, no leading slash)
    """
    # Normalise to platform-aware Path objects
    full = Path(full_path).expanduser().resolve()
    base = Path(base_mount).expanduser().resolve()

    # 1) Get the sub-path *relative* to the mount
    try:
        rel_parts = full.relative_to(base)
    except ValueError:               # not under base_mount
        raise ValueError(f"{full} is not under {base}")

    # 2) Convert to POSIX form (forces forward slashes)
    return str(PurePosixPath(rel_parts))

def build_file_path(base_mount: str,
                    server_dir: str,
                    filename: str) -> Path:
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
    full_path = Path(base_mount).joinpath(*rel_parts, filename)

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