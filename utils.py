import hashlib

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