from dataclasses import dataclass
from pathlib import Path
import os

@dataclass
class Config:
    # API Konfiguration
    OPENAI_API_KEY: str = os.getenv('OPENAI_API_KEY', '')
    OPENAI_MODEL: str = os.getenv('OPENAI_MODEL', 'gpt-4-turbo-preview')
    KRAKEN_API_KEY: str = os.getenv('KRAKEN_API_KEY', '')
    KRAKEN_API_SECRET: str = os.getenv('KRAKEN_API_SECRET', '')
    TELEGRAM_BOT_TOKEN: str = os.getenv('TELEGRAM_BOT_TOKEN', '')
    TELEGRAM_CHAT_ID: str = os.getenv('TELEGRAM_CHAT_ID', '')
    
    # Pfade
    BASE_DIR: Path = Path(__file__).parent
    DATA_DIR: Path = BASE_DIR / "data"
    PORTFOLIO_DIR: Path = DATA_DIR / "portfolio" / "kraken"
    LLM_DIR: Path = DATA_DIR / "llm"
    
    # Konstanten
    MIN_ASSET_AMOUNT: float = 0.0001
    MAX_TELEGRAM_MESSAGE_LENGTH: int = 4000
    API_TIMEOUT: float = 60.0
    RATE_LIMIT_DELAY: float = 1.0 