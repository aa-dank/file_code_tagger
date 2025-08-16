# db/models.py

import logging
import os
from pathlib import Path, PurePosixPath
from pgvector.sqlalchemy import Vector
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, BigInteger, SmallInteger, Text, Boolean, Numeric, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from db.db import get_db_engine  # Import get_db_engine from db/db.py

logger = logging.getLogger(__name__)

Base = declarative_base()

# get_db_engine moved to db/db.py

class File(Base):
    __tablename__ = 'files'
    id = Column(Integer, primary_key=True)
    size = Column(BigInteger, nullable=False, comment="File size in bytes.")
    hash = Column(String, nullable=False, unique=True, comment="SHA1 File hash for integrity checks.")
    extension = Column(String)
    locations = relationship("FileLocation", back_populates="file", cascade="all, delete-orphan")
    tag_labels = relationship("FileTagLabel", back_populates="file", cascade="all, delete-orphan", foreign_keys="[FileTagLabel.file_hash]")
    embedding = relationship("FileEmbedding", back_populates="file", uselist=False, cascade="all, delete-orphan")
    collection_members = relationship("FileCollectionMember", back_populates="file", cascade="all, delete-orphan", foreign_keys="[FileCollectionMember.file_id]")

    date_mentions = relationship(
        "FileDateMention",
        back_populates="file",
        cascade="all, delete-orphan",
        foreign_keys="[FileDateMention.file_hash]"
    )


class FileLocation(Base):
    __tablename__ = 'file_locations'
    id = Column(Integer, primary_key=True)
    file_id = Column(Integer, ForeignKey('files.id'), nullable=False)
    existence_confirmed = Column(DateTime(timezone=True))
    hash_confirmed = Column(DateTime(timezone=True))
    file_server_directories = Column(String)
    filename = Column(String)
    file = relationship("File", back_populates="locations")

    def local_filepath(self, server_mount_path: str) -> Path:
        if not self.file_server_directories or not self.filename:
            return None
        if not hasattr(self, '_local_path'):
            rel_parts = PurePosixPath(self.file_server_directories).parts
            self._local_path = Path(server_mount_path).joinpath(*rel_parts, self.filename)
        return self._local_path

    @property
    def file_size(self) -> int:
        file = self.file
        return file.size if file else 0


class FilingTag(Base):
    __tablename__ = 'filing_tags'
    label = Column(Text, primary_key=True)
    parent_label = Column(Text, ForeignKey('filing_tags.label'))
    description = Column(Text)
    importance_rank = Column(Integer)
    confidence_floor = Column(Numeric, default=0.60)
    parent = relationship("FilingTag", remote_side=[label], back_populates="children")
    children = relationship("FilingTag", back_populates="parent")
    file_labels = relationship("FileTagLabel", back_populates="filing_tag")
    prototypes = relationship(
        "TagPrototype",
        back_populates="filing_tag",
        cascade="all, delete-orphan",
        single_parent=True,
        order_by="TagPrototype.prototype_id",
    )

    @property
    def full_tag_label_str(self) -> str:
        return f"{self.label} - {self.description}".strip()

    @property
    def label_search_str(self) -> str:
        return f"{self.label} - "

    @classmethod
    def retrieve_tag_by_label(cls, session, label_str: str) -> 'FilingTag':
        if ' ' in label_str:
            label_str = label_str.split(' ')[0]
        return session.query(cls).filter_by(label=label_str).first()


class FileTagLabel(Base):
    __tablename__ = 'file_tag_labels'
    file_id = Column(Integer, ForeignKey('files.id'), primary_key=True)
    file_hash = Column(String, ForeignKey('files.hash'), nullable=False)
    tag = Column(Text, ForeignKey('filing_tags.label'), primary_key=True)
    is_primary = Column(Boolean, default=True)
    label_source = Column(Text, default='human')
    split = Column(Text, default='train')
    file = relationship("File", back_populates="tag_labels", foreign_keys=[file_hash])
    filing_tag = relationship("FilingTag", back_populates="file_labels")


class FileEmbedding(Base):
    __tablename__ = 'file_embeddings'
    __table_args__ = (
        Index('ix_file_embeddings_minilm_emb', 'minilm_emb', postgresql_using='ivfflat', postgresql_ops={'minilm_emb': 'vector_cosine_ops'}, postgresql_with={'lists': 100}),
        Index('ix_file_embeddings_mpnet_emb', 'mpnet_emb', postgresql_using='ivfflat', postgresql_ops={'mpnet_emb': 'vector_cosine_ops'}, postgresql_with={'lists': 100}),
    )
    file_hash = Column(String, ForeignKey('files.hash'), primary_key=True)
    source_text = Column(Text)
    minilm_model = Column(Text)
    minilm_emb = Column(Vector(384))
    mpnet_model = Column(Text)
    mpnet_emb = Column(Vector(768))
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    file = relationship("File", back_populates="embedding", foreign_keys=[file_hash])


class TagPrototype(Base):
    __tablename__ = "tag_prototypes"
    __table_args__ = (
        Index(
            "ix_tag_prototypes_embedding",
            "embedding",
            postgresql_using="ivfflat",
            postgresql_ops={"embedding": "vector_cosine_ops"},
            postgresql_with={"lists": 100},
        ),
    )
    tag = Column(Text, ForeignKey("filing_tags.label", ondelete="CASCADE"), primary_key=True)
    prototype_id = Column(SmallInteger, primary_key=True, default=0)
    model_name   = Column(Text, nullable=False)
    embedding    = Column(Vector(768), nullable=False)
    doc_count    = Column(Integer)
    updated_at   = Column(DateTime(timezone=True), server_default=func.now())
    notes        = Column(Text, comment="Optional notes about the prototype.")
    filing_tag = relationship("FilingTag", back_populates="prototypes")


class PrototypeRun(Base):
    __tablename__ = "prototype_runs"
    run_id        = Column(Integer, primary_key=True)
    model_name    = Column(Text, nullable=False)
    model_version = Column(Text, nullable=False)
    algorithm     = Column(Text, nullable=False)
    hyperparams   = Column(JSONB, nullable=True)
    tag_filter    = Column(Text, nullable=True)
    created_at    = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
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
    __tablename__ = "prototype_members"
    run_id = Column(Integer, ForeignKey("prototype_runs.run_id", ondelete="CASCADE"), primary_key=True)
    tag = Column(Text, ForeignKey("filing_tags.label", ondelete="CASCADE"), primary_key=True)
    prototype_id = Column(SmallInteger, primary_key=True, default=0)
    file_id = Column(Integer, ForeignKey("files.id", ondelete="CASCADE"), primary_key=True)
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
    __tablename__ = "prototype_run_metrics"
    run_id = Column(Integer, ForeignKey("prototype_runs.run_id", ondelete="CASCADE"), primary_key=True)
    metric_name = Column(Text, primary_key=True)
    split = Column(Text, primary_key=True)
    value = Column(Numeric)
    computed_at = Column(DateTime(timezone=True), server_default=func.now())
    run = relationship("PrototypeRun", back_populates="metrics")


class FileCollection(Base):
    __tablename__ = 'file_collections'
    id          = Column(Integer, primary_key=True)
    name        = Column(Text, nullable=False, unique=True)
    description = Column(Text, comment="Human-friendly notes about this collection")
    created_at  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    members = relationship(
        "FileCollectionMember",
        back_populates="collection",
        cascade="all, delete-orphan"
    )


class FileCollectionMember(Base):
    __tablename__ = 'file_collection_members'
    collection_id = Column(Integer, ForeignKey('file_collections.id', ondelete='CASCADE'), primary_key=True)
    file_id       = Column(Integer, ForeignKey('files.id', ondelete='CASCADE'), primary_key=True)
    role          = Column(Text, nullable=False, comment="Role of this file in the collection: 'train', 'test', 'val', 'prototype', etc.")
    added_at      = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    collection = relationship(
        "FileCollection",
        back_populates="members"
    )
    file = relationship(
        "File",
        back_populates="collection_members"
    )


class FileDateMention(Base):
    __tablename__ = 'file_date_mentions'
    __table_args__ = (
        Index('ix_file_date_mentions_date', 'mention_date'),
        Index('ix_file_date_mentions_file', 'file_hash'),
        Index('ix_file_date_mentions_date_gran', 'mention_date', 'granularity'),
    )

    # Link to files via the unique file hash (consistent with FileEmbedding / FileTagLabel)
    file_hash     = Column(String, ForeignKey('files.hash'), primary_key=True)
    mention_date  = Column(Date, primary_key=True)           # normalized calendar date
    granularity   = Column(Text, primary_key=True, default='day')  # 'day' | 'month' | 'year'
    mentions_count= Column(Integer, nullable=False, default=1)     # per-file count for this date
    extractor     = Column(Text, nullable=True)              # e.g., 'dateparser@1.2.3'
    extracted_at  = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    file = relationship("File", back_populates="date_mentions", foreign_keys=[file_hash])
