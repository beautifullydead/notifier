from dataclasses import dataclass
from dataclasses_json import dataclass_json
from typing import List, Optional, Dict, Any
from sqlalchemy import Column, Float, String, DateTime, Integer, select, create_engine, Boolean, event
from sqlalchemy.orm import Session, Mapped, mapped_column, DeclarativeBase, MappedAsDataclass, sessionmaker
from sqlalchemy.exc import SQLAlchemyError, OperationalError
from sqlalchemy.pool import QueuePool
import logging
import re
from contextlib import contextmanager

logger = logging.getLogger(__name__)

@dataclass_json
@dataclass
class EmailConfig:
    enabled: bool
    smtp_server: str
    smtp_port: int
    smtp_use_tls: bool
    imap_server: str
    imap_port: int
    username: str
    password: str
    from_address: str
    to_addresses: List[str]
    notification_subject_prefix: str

    def validate(self) -> List[str]:
        """Validate email configuration"""
        errors = []
        if self.enabled:
            if not self.smtp_server:
                errors.append("SMTP server is required when email is enabled")
            if not self.smtp_port:
                errors.append("SMTP port is required when email is enabled")
            if not self.username:
                errors.append("Username is required when email is enabled")
            if not self.password:
                errors.append("Password is required when email is enabled")
            if not self.from_address:
                errors.append("From address is required when email is enabled")
            if not self.to_addresses:
                errors.append("At least one recipient address is required when email is enabled")
            else:
                email_pattern = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                for email in [self.from_address] + self.to_addresses:
                    if not email_pattern.match(email):
                        errors.append(f"Invalid email address format: {email}")
        return errors


@dataclass_json
@dataclass
class Config:
    urls: List[str]
    email: EmailConfig
    db_user: str
    db_password: str
    filters: List[str]
    combine_notifications: bool = True
    notification_cooldown: int = 300
    max_results_per_search: int = 20
    db_host: str = 'localhost'
    db_port: str = '5432'
    db_name: str = 'craigslist'
    db_pool_size: int = 5
    db_max_overflow: int = 10
    db_pool_timeout: int = 30

    def __post_init__(self):
        """Convert EmailConfig dict to object if needed"""
        if isinstance(self.email, dict):
            self.email = EmailConfig(**self.email)

    def validate(self) -> List[str]:
        """Validate configuration"""
        errors = []
        
        # Validate URLs
        if not self.urls:
            errors.append("At least one URL is required")
        else:
            url_pattern = re.compile(r'^https?://[^\s/$.?#].[^\s]*$')
            for url in self.urls:
                if not url_pattern.match(url):
                    errors.append(f"Invalid URL format: {url}")

        # Validate database settings
        if not self.db_user:
            errors.append("Database user is required")
        if not self.db_password:
            errors.append("Database password is required")
            
        # Validate email configuration
        email_errors = self.email.validate()
        errors.extend(email_errors)

        return errors


class DatabaseError(Exception):
    """Custom exception for database errors"""
    pass


def create_db_url(config: Config) -> str:
    """Create database URL from config"""
    return f'postgresql://{config.db_user}:{config.db_password}@{config.db_host}:{config.db_port}/{config.db_name}'


def get_engine(user: str = 'postgres',
              password: str = 'password',
              host: str = 'localhost',
              port: str = '5432',
              database: str = 'craigslist',
              echo: bool = False,
              pool_size: int = 5,
              max_overflow: int = 10,
              pool_timeout: int = 30):
    """Create database engine with connection pooling"""
    try:
        SQLALCHEMY_DATABASE_URL = f'postgresql://{user}:{password}@{host}:{port}/{database}'
        engine = create_engine(
            SQLALCHEMY_DATABASE_URL,
            echo=echo,
            poolclass=QueuePool,
            pool_size=pool_size,
            max_overflow=max_overflow,
            pool_timeout=pool_timeout
        )

        # Add event listeners for connection debugging
        @event.listens_for(engine, 'connect')
        def connect(dbapi_connection, connection_record):
            logger.debug('Database connection established')

        @event.listens_for(engine, 'checkout')
        def checkout(dbapi_connection, connection_record, connection_proxy):
            logger.debug('Database connection retrieved from pool')

        @event.listens_for(engine, 'checkin')
        def checkin(dbapi_connection, connection_record):
            logger.debug('Database connection returned to pool')

        return engine
    except Exception as e:
        logger.error(f"Failed to create database engine: {str(e)}")
        raise DatabaseError(f"Database connection failed: {str(e)}")


class Base(MappedAsDataclass, DeclarativeBase):
    """Base class for SQLAlchemy models"""
    pass


def get_db(table_name: str):
    """Create database model for listings"""
    class db_listing_entry(Base):
        __tablename__ = f'cl_table_{table_name}'
        
        id: Mapped[int] = mapped_column(Integer, init=False, primary_key=True)
        link: Mapped[str] = mapped_column(String, index=True)
        title: Mapped[str] = mapped_column(String)
        cl_id: Mapped[str] = mapped_column(String, index=True, unique=True)
        screenshot_path: Mapped[Optional[str]] = mapped_column(String)
        time_posted: Mapped[str] = mapped_column(String, index=True)
        location: Mapped[str] = mapped_column(String, index=True)
        time_scraped: Mapped[str] = mapped_column(String)
        notified: Mapped[bool] = mapped_column(Boolean, default=False, index=True)

        def __repr__(self):
            return (f'Listing(title={self.title}, '
                   f'id={self.cl_id}, '
                   f'location={self.location}, '
                   f'time_posted={self.time_posted})')

    return db_listing_entry


@contextmanager
def session_scope(engine) -> Session:
    """Provide a transactional scope around a series of operations."""
    session = sessionmaker(bind=engine)()
    try:
        yield session
        session.commit()
    except SQLAlchemyError as e:
        logger.error(f"Database error: {str(e)}")
        session.rollback()
        raise DatabaseError(f"Database operation failed: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during database operation: {str(e)}")
        session.rollback()
        raise
    finally:
        session.close()


def init_db(engine) -> None:
    """Initialize database tables"""
    try:
        Base.metadata.create_all(bind=engine)
        logger.info("Database tables created successfully")
    except SQLAlchemyError as e:
        logger.error(f"Failed to create database tables: {str(e)}")
        raise DatabaseError(f"Database initialization failed: {str(e)}")
