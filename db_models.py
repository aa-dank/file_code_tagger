# Database Models

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, SmallInteger, Text, Boolean, Numeric, Index, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.dialects.postgresql import VECTOR
from sqlalchemy.sql import func

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
    size = Column(BigInteger)
    hash = Column(String, nullable=False, unique=True,
                  comment="SHA1 File hash for integrity checks.")
    extension = Column(String)
    
    # Relationship to file_locations
    locations = relationship("FileLocation", back_populates="file", cascade="all, delete-orphan")
    # Relationship to file_tag_labels
    tag_labels = relationship("FileTagLabel", back_populates="file", cascade="all, delete-orphan")
    # Relationship to file_embeddings
    embedding = relationship("FileEmbedding", back_populates="file", uselist=False, cascade="all, delete-orphan")


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
    existence_confirmed = Column(DateTime)
    hash_confirmed = Column(DateTime)
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
    file = relationship("File", back_populates="tag_labels")
    filing_tag = relationship("FilingTag", back_populates="file_labels")


class FileEmbedding(Base):
    """
    File embeddings for semantic distance search.
    
    PostgreSQL equivalent:
    CREATE TABLE file_embeddings (
      file_id            INTEGER PRIMARY KEY REFERENCES files(id),
      source_text        TEXT,                     -- OCR/plain text cache
      minilm_model       TEXT    DEFAULT 'all-MiniLM-L6-v2',
      minilm_emb         VECTOR(384),
      mpnet_model        TEXT,
      mpnet_emb          VECTOR(768),
      updated_at         TIMESTAMPTZ DEFAULT now()
    );
    
    -- Indexes for ANN search (one per VECTOR column)
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
    
    file_id = Column(Integer, ForeignKey('files.id'), primary_key=True)
    source_text = Column(Text)
    minilm_model = Column(Text)
    minilm_emb = Column(VECTOR(384))
    mpnet_model = Column(Text)
    mpnet_emb = Column(VECTOR(768))
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())
    
    # Relationship
    file = relationship("File", back_populates="embedding")


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
      embedding     VECTOR() NOT NULL,      -- any dimension
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
    embedding    = Column(VECTOR(), nullable=False)  # any dimension
    doc_count    = Column(Integer)
    updated_at   = Column(DateTime, server_default=func.now())
    notes        = Column(Text,
                          comment="Optional notes about the prototype.")

    # ORM back-ref
    filing_tag = relationship("FilingTag", back_populates="prototypes")