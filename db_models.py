# Database Models

from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, Text, Boolean, Numeric, Index
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
    hash = Column(String)
    extension = Column(String)
    
    # Relationship to file_locations
    locations = relationship("FileLocation", back_populates="file", cascade="all, delete-orphan")
    # Relationship to file_code_labels
    code_labels = relationship("FileCodeLabel", back_populates="file", cascade="all, delete-orphan")
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

class FilingCode(Base):
    """
    Filing codes for categorizing files.
    
    PostgreSQL equivalent:
    CREATE TABLE filing_codes (
      code              TEXT  PRIMARY KEY,      -- 'F7.1'
      parent_code       TEXT REFERENCES filing_codes(code),
      description       TEXT,
      importance_rank   INTEGER,               -- 1 = very important
      confidence_floor  NUMERIC DEFAULT 0.60   -- per-code threshold
    );
    """
    __tablename__ = 'filing_codes'
    
    code = Column(Text, primary_key=True)
    parent_code = Column(Text, ForeignKey('filing_codes.code'))
    description = Column(Text)
    importance_rank = Column(Integer)
    confidence_floor = Column(Numeric, default=0.60)
    
    # Self-referential relationship
    parent = relationship("FilingCode", remote_side=[code], back_populates="children")
    children = relationship("FilingCode", back_populates="parent")
    
    # Relationship to file_code_labels
    file_labels = relationship("FileCodeLabel", back_populates="filing_code")

class FileCodeLabel(Base):
    """
    Labels connecting files to filing codes.
    
    PostgreSQL equivalent:
    CREATE TABLE file_code_labels (
      file_id       INTEGER REFERENCES files(id),
      code          TEXT    REFERENCES filing_codes(code),
      is_primary    BOOLEAN DEFAULT TRUE,   -- leaf vs ancestor tag
      label_source  TEXT    DEFAULT 'human',-- 'human', 'rule', 'model'
      split         TEXT    DEFAULT 'train',-- 'train', 'test', 'val'
      PRIMARY KEY (file_id, code)
    );
    """
    __tablename__ = 'file_code_labels'
    
    file_id = Column(Integer, ForeignKey('files.id'), primary_key=True)
    code = Column(Text, ForeignKey('filing_codes.code'), primary_key=True)
    is_primary = Column(Boolean, default=True)
    label_source = Column(Text, default='human')
    split = Column(Text, default='train')
    
    # Relationships
    file = relationship("File", back_populates="code_labels")
    filing_code = relationship("FilingCode", back_populates="file_labels")

class FileEmbedding(Base):
    """
    File embeddings for vector similarity search.
    
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
    
    file_id = Column(Integer, ForeignKey('files.id'), primary_key=True)
    source_text = Column(Text)
    minilm_model = Column(Text, default='all-MiniLM-L6-v2')
    minilm_emb = Column(VECTOR(384))
    mpnet_model = Column(Text)
    mpnet_emb = Column(VECTOR(768))
    updated_at = Column(DateTime, default=func.now())
    
    # Relationship
    file = relationship("File", back_populates="embedding")

# Create indexes for vector columns
Index('ix_file_embeddings_minilm_emb', FileEmbedding.minilm_emb, postgresql_using='ivfflat', postgresql_ops={'minilm_emb': 'vector_cosine_ops'}, postgresql_with={'lists': 100})
Index('ix_file_embeddings_mpnet_emb', FileEmbedding.mpnet_emb, postgresql_using='ivfflat', postgresql_ops={'mpnet_emb': 'vector_cosine_ops'}, postgresql_with={'lists': 100})