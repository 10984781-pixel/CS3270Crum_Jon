"""Data tier for the Phase 9 Flask app (SQLite + SQLAlchemy)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import Boolean, DateTime, Integer, String, create_engine, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker


class Base(DeclarativeBase):
    """Base class for SQLAlchemy models."""


class QueryHistory(Base):
    """Persisted user query history."""

    __tablename__ = "query_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    location: Mapped[str] = mapped_column(String(120), nullable=False)
    rainy_only: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    result_count: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


@dataclass(frozen=True)
class QueryHistoryItem:
    """Read model exposed to the service/presentation tiers."""

    id: int
    location: str
    rainy_only: bool
    result_count: int
    created_at: datetime


class Database:
    """Database setup and session factory."""

    def __init__(self, db_path: str | Path):
        path = Path(db_path)
        self.engine = create_engine(f"sqlite:///{path}", future=True)
        self.session_factory = sessionmaker(
            bind=self.engine,
            autoflush=False,
            expire_on_commit=False,
            future=True,
        )

    def create_tables(self) -> None:
        Base.metadata.create_all(self.engine)


class QueryHistoryRepository:
    """Repository for query history persistence operations."""

    def __init__(self, session_factory):
        self.session_factory = session_factory

    def add_query(self, location: str, rainy_only: bool, result_count: int) -> None:
        with self.session_factory() as session:
            entry = QueryHistory(
                location=location,
                rainy_only=rainy_only,
                result_count=result_count,
            )
            session.add(entry)
            session.commit()

    def recent_queries(self, limit: int = 10) -> list[QueryHistoryItem]:
        with self.session_factory() as session:
            query = (
                select(QueryHistory)
                .order_by(QueryHistory.created_at.desc(), QueryHistory.id.desc())
                .limit(limit)
            )
            records = session.execute(query).scalars().all()
            return [
                QueryHistoryItem(
                    id=record.id,
                    location=record.location,
                    rainy_only=record.rainy_only,
                    result_count=record.result_count,
                    created_at=record.created_at,
                )
                for record in records
            ]
