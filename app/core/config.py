import os
from dotenv import load_dotenv
from pydantic import BaseModel, Field

# Load environment variables from .env file if it exists
load_dotenv()

class Settings(BaseModel):
    REDIS_URL: str = Field(default_factory=lambda: os.getenv("REDIS_URL", "redis://localhost:6379"))
    OPENAI_API_KEY: str = Field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    GEMINI_API_KEY: str = Field(default_factory=lambda: os.getenv("GEMINI_API_KEY", ""))
    DEFAULT_EMBEDDING_MODEL: str = Field(default_factory=lambda: os.getenv("DEFAULT_EMBEDDING_MODEL", "sentence-transformers/all-MiniLM-L6-v2"))
    CACHE_SIMILARITY_THRESHOLD: float = Field(default_factory=lambda: float(os.getenv("CACHE_SIMILARITY_THRESHOLD", "0.90")))
    DEFAULT_USER_CAPACITY: float = Field(default_factory=lambda: float(os.getenv("DEFAULT_USER_CAPACITY", "10000")))
    DEFAULT_USER_REFILL_RATE: float = Field(default_factory=lambda: float(os.getenv("DEFAULT_USER_REFILL_RATE", "100")))

settings = Settings()
