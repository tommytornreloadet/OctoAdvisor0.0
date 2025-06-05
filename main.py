#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OctoAdvisor - Hauptmodul

Dieses Skript koordiniert den gesamten Workflow:
1. Abrufen des Krypto-Portfolios von Kraken
2. Speichern der Portfoliodaten
3. Analyse des Portfolios mit einem KI-Modell
4. Speichern der Analyseergebnisse
5. Senden der Analyse über Telegram

Das Skript ist für die Ausführung über einen Cronjob konzipiert.
"""

import os
import sys
import logging
from datetime import datetime
from dotenv import load_dotenv
from pathlib import Path
from config import Config

# Eigene Module importieren
import kraken
import analysis
import telegram_bot

# Logging konfigurieren
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("octoadvisor.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("OctoAdvisor")

def setup_directories(config: Config) -> None:
    """Erstellt die benötigten Verzeichnisse."""
    directories = [config.DATA_DIR, config.PORTFOLIO_DIR, config.LLM_DIR]
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Verzeichnis sichergestellt: {directory}")

def check_prompt_file(config: Config) -> None:
    """Überprüft, ob die Prompt-Datei existiert."""
    prompt_file = config.BASE_DIR / "prompt.txt"
    if not prompt_file.exists():
        raise FileNotFoundError(f"Prompt-Datei nicht gefunden: {prompt_file}")

def validate_environment() -> None:
    """Validiert alle erforderlichen Umgebungsvariablen."""
    required_vars = [
        'OPENAI_API_KEY',
        'KRAKEN_API_KEY', 
        'KRAKEN_API_SECRET',
        'TELEGRAM_BOT_TOKEN',
        'TELEGRAM_CHAT_ID'
    ]
    
    missing = [var for var in required_vars if not os.getenv(var)]
    if missing:
        raise ValueError(f"Fehlende Umgebungsvariablen: {', '.join(missing)}")

def main():
    """Hauptfunktion, die den gesamten Workflow ausführt."""
    try:
        config = Config()
        load_dotenv()
        
        setup_directories(config)
        check_prompt_file(config)
        
        # Zeitstempel für Dateinamen erstellen
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 1. Portfolio von Kraken abrufen
        logger.info("Rufe Portfolio-Daten von Kraken ab...")
        portfolio_data = kraken.get_portfolio_simple()
        
        print("DEBUG - Raw Portfolio Data:", portfolio_data)  # Debug hinzufügen
        
        # 2. Portfolio-Daten speichern
        portfolio_filename = f"data/portfolio/kraken/portfolio_{timestamp}.json"
        kraken.save_portfolio(portfolio_data, portfolio_filename)
        logger.info(f"Portfolio-Daten gespeichert in {portfolio_filename}")
        
        # 3. KI-Analyse durchführen
        logger.info("Führe KI-Analyse durch...")
        with open("prompt.txt", "r", encoding="utf-8") as f:
            prompt_template = f.read()
        
        # Portfolio-Daten für die Analyse vorbereiten
        logger.info("Bereite Portfolio-Daten für Analyse vor...")
        formatted_portfolio = analysis.prepare_portfolio_for_analysis(portfolio_data)
        
        print("DEBUG - Formatted Portfolio:", formatted_portfolio)  # Debug hinzufügen
        
        # KI-Analyse durchführen - KORRIGIERT: formatted_portfolio statt portfolio_data
        analysis_result = analysis.analyze_portfolio(formatted_portfolio, prompt_template)
        
        # 4. Analyseergebnis speichern
        analysis_filename = f"data/llm/analysis_{timestamp}.txt"
        analysis.save_analysis(analysis_result, analysis_filename)
        logger.info(f"Analyseergebnis gespeichert in {analysis_filename}")
        
        # 5. Analyse über Telegram senden
        logger.info("Sende Analyse über Telegram...")
        telegram_bot.send_message(analysis_result)
        logger.info("Analyse erfolgreich über Telegram gesendet")
        
        logger.info("OctoAdvisor-Durchlauf erfolgreich abgeschlossen")
        
    except Exception as e:
        logger.error(f"Fehler im Hauptprogramm: {str(e)}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main() 