# knn/base.py

import numpy as np


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