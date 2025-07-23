# Database Models

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, SmallInteger, Text, Boolean, Numeric, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from pgvector.sqlalchemy import Vector


Base = declarative_base()

class File(Base):
    """
    Main files table storing file metadata.
      
    PostgreSQL equivalent:
    CREATE TABLE files (
      id        INTEGER PRIMARY KEY,
      size      BIGINT,
      hash      CHARACTER VARYING,
      extension CHARACTER VARYING
    );
    """
    __tablename__ = 'files'
    
    id = Column(Integer, primary_key=True)
    size = Column(BigInteger, nullable=False,
                  comment="File size in bytes.")
    hash = Column(String, nullable=False, unique=True,
                  comment="SHA1 File hash for integrity checks.")
    extension = Column(String)
    
    # Relationship to file_locations
    locations = relationship("FileLocation",
                             back_populates="file",
                             cascade="all, delete-orphan")
    # Relationship to file_tag_labels
    tag_labels = relationship("FileTagLabel",
                              back_populates="file",
                              cascade="all, delete-orphan",
                              foreign_keys="[FileTagLabel.file_hash]")
    # Relationship to file_embeddings
    embedding = relationship("FileEmbedding",
                             back_populates="file",
                             uselist=False,
                             cascade="all, delete-orphan")


class FileLocation(Base):
    """
    File locations table tracking where files are stored.
    
    PostgreSQL equivalent:
    CREATE TABLE file_locations (
      id                      INTEGER PRIMARY KEY,
      file_id                 INTEGER NOT NULL REFERENCES files(id),
      existence_confirmed     TIMESTAMP WITHOUT TIME ZONE,
      hash_confirmed          TIMESTAMP WITHOUT TIME ZONE,
      file_server_directories CHARACTER VARYING,
      filename                CHARACTER VARYING
    );
    """
    __tablename__ = 'file_locations'
    
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    existence_confirmed = Column(DateTime(timezone=True))
    hash_confirmed = Column(DateTime(timezone=True))
    file_server_directories = Column(String)
    filename = Column(String)
    
    # Relationship to files
    file = relationship("File", back_populates="locations")


class FilingTag(Base):
    """
    Filing tags for categorizing files.

    PostgreSQL equivalent:
    CREATE TABLE filing_tags (
      label              TEXT  PRIMARY KEY,      -- 'F7.1'
      parent_label       TEXT REFERENCES filing_tags(label),
      description       TEXT,
      importance_rank   INTEGER,               -- 1 = very important
      confidence_floor  NUMERIC DEFAULT 0.60   -- per-label threshold
    );
    """
    __tablename__ = 'filing_tags'

    label = Column(Text, primary_key=True)
    parent_label = Column(Text, ForeignKey('filing_tags.label'))
    description = Column(Text)
    importance_rank = Column(Integer)
    confidence_floor = Column(Numeric, default=0.60)
    
    # Self-referential relationship
    parent = relationship("FilingTag", remote_side=[label], back_populates="children")
    children = relationship("FilingTag", back_populates="parent")

    # Relationship to file_tag_labels
    file_labels = relationship("FileTagLabel", back_populates="filing_tag")

    prototypes = relationship(
        "TagPrototype",
        back_populates="filing_tag",
        cascade="all, delete-orphan",
        single_parent=True,   # guarantees a prototype row can’t be re-parented
        order_by="TagPrototype.prototype_id",
    )


class FileTagLabel(Base):
    """
    Labels connecting files to filing tags.

    PostgreSQL equivalent:
    CREATE TABLE file_tag_labels (
      file_id       INTEGER REFERENCES files(id),
      file_hash     VARCHAR REFERENCES files(hash),
      tag           TEXT    REFERENCES filing_tags(label),
      is_primary   BOOLEAN DEFAULT TRUE,   -- leaf vs ancestor tag 
      label_source  TEXT,                     -- 'human', 'rule', 'model'
      split         TEXT,                     -- 'train', 'test', 'val'
      PRIMARY KEY (file_id, tag)
    );
    """
    __tablename__ = 'file_tag_labels'

    file_id = Column(Integer, ForeignKey('files.id'),
                     primary_key=True,
                     comment="File ID - primary key from files table.")
    file_hash = Column(String, ForeignKey('files.hash'), nullable=False,
                       comment="File hash - foreign key to files table.")
    tag = Column(Text, ForeignKey('filing_tags.label'),
                 primary_key=True,
                 comment="Filing tag label. This is a foreign key to the filing_tags table.")
    is_primary = Column(Boolean,
                        default=True,
                        comment = "Leaf vs ancestor tag (primary = leaf) - distinguishes 'explicitly assigned leaf tag' from 'inherited parent tag.'")
    label_source = Column(Text,
                          default='human',
                          comment = "Source of the label - 'human', 'rule', 'model'")
    split = Column(Text,
                   default='train',
                   comment = "Data split - 'train', 'test', 'val'")

    # Relationships
    file = relationship("File", back_populates="tag_labels", foreign_keys=[file_hash])
    filing_tag = relationship("FilingTag", back_populates="file_labels")


class FileEmbedding(Base):
    """
    File embeddings for semantic distance search.
    
    PostgreSQL equivalent:
    CREATE TABLE file_embeddings (
      file_id            INTEGER PRIMARY KEY REFERENCES files(id),
      file_hash          VARCHAR NOT NULL REFERENCES files(hash),
      source_text        TEXT,                     -- OCR/plain text cache
      minilm_model       TEXT    DEFAULT 'all-MiniLM-L6-v2',
      minilm_emb         Vector(384),
      mpnet_model        TEXT,
      mpnet_emb          Vector(768),
      updated_at         TIMESTAMPTZ DEFAULT now()
    );

    -- Indexes for ANN search (one per Vector column)
    CREATE INDEX ON file_embeddings
      USING ivfflat (minilm_emb vector_cosine_ops) WITH (lists = 100);
    
    CREATE INDEX ON file_embeddings
      USING ivfflat (mpnet_emb vector_cosine_ops)  WITH (lists = 100);
    """
    __tablename__ = 'file_embeddings'
    __table_args__ = (
        Index('ix_file_embeddings_minilm_emb', 'minilm_emb', 
              postgresql_using='ivfflat', 
              postgresql_ops={'minilm_emb': 'vector_cosine_ops'}, 
              postgresql_with={'lists': 100}),
        Index('ix_file_embeddings_mpnet_emb', 'mpnet_emb', 
              postgresql_using='ivfflat', 
              postgresql_ops={'mpnet_emb': 'vector_cosine_ops'}, 
              postgresql_with={'lists': 100}),
    )
    
    file_hash = Column(String, ForeignKey('files.hash'), primary_key=True)
    source_text = Column(Text)
    minilm_model = Column(Text)
    minilm_emb = Column(Vector(384))
    mpnet_model = Column(Text)
    mpnet_emb = Column(Vector(768))
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    
    # Relationship
    file = relationship("File", back_populates="embedding", foreign_keys=[file_hash])


class TagPrototype(Base):
    """
    A prototype embedding for a filing tag.
    prototype_id = 0  → global centroid
                  >0 → sub-cluster or learned prototype
    
    PostgreSQL equivalent:
    CREATE TABLE tag_prototypes (
      tag           TEXT REFERENCES filing_tags(label) ON DELETE CASCADE,
      prototype_id  SMALLINT DEFAULT 0,     -- 0 = centroid
      model_name    TEXT NOT NULL,
      embedding     Vector(768) NOT NULL,   -- fixed dimension
      doc_count     INTEGER,
      updated_at    TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (tag, prototype_id)
    );
    
    -- Index for ANN search
    CREATE INDEX ix_tag_prototypes_embedding ON tag_prototypes
      USING ivfflat (embedding vector_cosine_ops) WITH (lists = 100);
    """
    __tablename__ = "tag_prototypes"
    __table_args__ = (
        # fast ANN search when you want to query prototypes directly
        Index(
            "ix_tag_prototypes_embedding",
            "embedding",
            postgresql_using="ivfflat",
            postgresql_ops={"embedding": "vector_cosine_ops"},
            postgresql_with={"lists": 100},
        ),
    )

    tag = Column(
        Text,
        ForeignKey("filing_tags.label", ondelete="CASCADE"),
        primary_key=True,
    )
    prototype_id = Column(SmallInteger, primary_key=True, default=0)  # 0 = centroid
    model_name   = Column(Text, nullable=False)
    embedding    = Column(Vector(768), nullable=False)  # fixed dimension
    doc_count    = Column(Integer)
    updated_at   = Column(DateTime(timezone=True), server_default=func.now())
    notes        = Column(Text,
                          comment="Optional notes about the prototype.")

    # ORM back-ref
    filing_tag = relationship("FilingTag", back_populates="prototypes")


class PrototypeRun(Base):
    """
    Records a single prototype-generation experiment.

    PostgreSQL equivalent:
    CREATE TABLE prototype_runs (
      run_id        SERIAL PRIMARY KEY,
      model_name    TEXT NOT NULL,
      model_version TEXT NOT NULL,
      algorithm     TEXT NOT NULL,
      hyperparams   JSONB,
      tag_filter    TEXT,
      created_at    TIMESTAMPTZ DEFAULT now()
    );

    Each row captures the configuration and context used to generate
    one set of TagPrototypes (centroids or sub-clusters) for all filing tags.

    Attributes:
        run_id         – Auto-incrementing surrogate key.
        model_name     – Name of the embedding model (e.g. 'all-MiniLM-L6-v2').
        model_version  – Version or date of the model (e.g. '2025-07-21').
        algorithm      – Prototype algorithm (e.g. 'mean', 'k-means-centroid').
        hyperparams    – JSON blob of algorithm parameters (e.g. {"k":5}).
        tag_filter     – Optional SQL WHERE fragment used to select tags/files.
        created_at     – Timestamp when this run was created.
        members        – Relationship to PrototypeMember entries.
        metrics        – Relationship to PrototypeRunMetric entries.
    """
    __tablename__ = "prototype_runs"

    run_id        = Column(Integer, primary_key=True)
    model_name    = Column(Text, nullable=False)
    model_version = Column(Text, nullable=False)
    algorithm     = Column(Text, nullable=False)
    hyperparams   = Column(JSONB, nullable=True)
    tag_filter    = Column(Text, nullable=True)
    created_at    = Column(DateTime(timezone=True),
                           server_default=func.now(), nullable=False)

    # Relationships
    members = relationship(
        "PrototypeMember",
        back_populates="run",
        cascade="all, delete-orphan"
    )
    metrics = relationship(
        "PrototypeRunMetric",
        back_populates="run",
        cascade="all, delete-orphan"
    )


class PrototypeMember(Base):
    """
    Links every **source file** to the prototype it contributed to *within a
    specific run*.

    PostgreSQL equivalent:
    CREATE TABLE prototype_members (
      run_id       INTEGER REFERENCES prototype_runs(run_id) ON DELETE CASCADE,
      tag          TEXT REFERENCES filing_tags(label) ON DELETE CASCADE,
      prototype_id SMALLINT DEFAULT 0,
      file_id      INTEGER REFERENCES files(id) ON DELETE CASCADE,
      PRIMARY KEY (run_id, tag, prototype_id, file_id)
    );
    CREATE INDEX ix_prototype_members_run_tag_pid ON prototype_members(run_id, tag, prototype_id);

    Composite PK lets you store multiple sub-centroids per tag
    (``prototype_id > 0`` for k-means clusters, ``0`` for global centroid).

    Useful queries
    --------------
    •  Quickly list all training files for a tag/run:
         ``SELECT file_id FROM prototype_members
            WHERE run_id = :run AND tag = 'F7.1';``

    •  Diagnose outliers (distance of each member from its centroid).
    """

    __tablename__ = "prototype_members"

    run_id = Column(
        Integer,
        ForeignKey("prototype_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    tag = Column(
        Text,
        ForeignKey("filing_tags.label", ondelete="CASCADE"),
        primary_key=True,
    )
    prototype_id = Column(SmallInteger, primary_key=True, default=0)
    file_id = Column(
        Integer,
        ForeignKey("files.id", ondelete="CASCADE"),
        primary_key=True,
    )

    # ─── Relationships ────────────────────────────────────────────────────────
    run = relationship("PrototypeRun", back_populates="members")
    filing_tag = relationship("FilingTag")
    file = relationship("File")

    __table_args__ = (
        Index(
            "ix_prototype_members_run_tag_pid",
            "run_id",
            "tag",
            "prototype_id",
        ),
    )


class PrototypeRunMetric(Base):
    """
    Stores **objective evaluation metrics** (accuracy, F1, MRR, etc.) for a
    given run on a particular data split.

    PostgreSQL equivalent:
    CREATE TABLE prototype_run_metrics (
      run_id      INTEGER REFERENCES prototype_runs(run_id) ON DELETE CASCADE,
      metric_name TEXT,
      split       TEXT,
      value       NUMERIC,
      computed_at TIMESTAMPTZ DEFAULT now(),
      PRIMARY KEY (run_id, metric_name, split)
    );

    Having metrics in SQL means you can:
    •  Grafana-dash accuracy over time.
    •  Programmatically pick the best run for deployment
       ``ORDER BY split='val', metric_name='micro_F1', value DESC LIMIT 1``.
    """

    __tablename__ = "prototype_run_metrics"

    run_id = Column(
        Integer,
        ForeignKey("prototype_runs.run_id", ondelete="CASCADE"),
        primary_key=True,
    )
    metric_name = Column(Text, primary_key=True)
    split = Column(Text, primary_key=True)  # 'train' | 'test' | 'val'
    value = Column(Numeric)
    computed_at = Column(DateTime(timezone=True), server_default=func.now())

    # ─── Relationships ────────────────────────────────────────────────────────
    run = relationship("PrototypeRun", back_populates="metrics")