-- 0. pgvector extension  (run once if not present)
CREATE EXTENSION IF NOT EXISTS vector;

-------------------------------------------------------------------------------
-- 1. filing_tags  (self-referential hierarchy)
-------------------------------------------------------------------------------
CREATE TABLE filing_tags (
    label            TEXT PRIMARY KEY,               -- e.g. 'F7.1'
    parent_label     TEXT REFERENCES filing_tags(label),
    description      TEXT,
    importance_rank  INTEGER,
    confidence_floor NUMERIC DEFAULT 0.60
);

-------------------------------------------------------------------------------
-- 2. file_tag_labels  (many-to-many file ↔ tag, dual FK: id + hash)
-------------------------------------------------------------------------------
CREATE TABLE file_tag_labels (
    file_id     INTEGER  NOT NULL
               REFERENCES files(id),
    file_hash   VARCHAR  NOT NULL
               REFERENCES files(hash),
    tag         TEXT     NOT NULL
               REFERENCES filing_tags(label),
    is_primary  BOOLEAN  DEFAULT TRUE,               -- leaf vs ancestor
    label_source TEXT    DEFAULT 'human',            -- 'human','rule','model'
    split        TEXT    DEFAULT 'train',            -- 'train','test','val'
    PRIMARY KEY (file_id, tag)
);

-- (optional but useful) fast lookup by hash
CREATE INDEX ix_file_tag_labels_hash_tag
  ON file_tag_labels (file_hash, tag);

-------------------------------------------------------------------------------
-- 3. file_embeddings  (wide-row, hash is PK)
-------------------------------------------------------------------------------
CREATE TABLE file_embeddings (
    file_hash   VARCHAR  PRIMARY KEY
               REFERENCES files(hash),
    source_text TEXT,
    minilm_model TEXT DEFAULT 'all-MiniLM-L6-v2',
    minilm_emb   VECTOR(384),
    mpnet_model  TEXT,
    mpnet_emb    VECTOR(768),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

-- ANN indexes (pgvector ≥0.5)
CREATE INDEX ix_file_embeddings_minilm_emb
  ON file_embeddings USING ivfflat (minilm_emb vector_cosine_ops)
  WITH (lists = 100);

CREATE INDEX ix_file_embeddings_mpnet_emb
  ON file_embeddings USING ivfflat (mpnet_emb vector_cosine_ops)
  WITH (lists = 100);

-------------------------------------------------------------------------------
-- 4. tag_prototypes  (per-tag centroids & sub-clusters)
-------------------------------------------------------------------------------
CREATE TABLE tag_prototypes (
    tag           TEXT      NOT NULL
                 REFERENCES filing_tags(label) ON DELETE CASCADE,
    prototype_id  SMALLINT  DEFAULT 0,
    model_name    TEXT      NOT NULL,
    embedding     VECTOR(768),
    doc_count     INTEGER,
    updated_at    TIMESTAMPTZ DEFAULT now(),
    notes         TEXT,
    PRIMARY KEY (tag, prototype_id)
);

CREATE INDEX ix_tag_prototypes_embedding
  ON tag_prototypes USING ivfflat (embedding vector_cosine_ops)
  WITH (lists = 100);
