#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OctoAdvisor - Analyse-Modul

Dieses Modul ist verantwortlich für die Analyse des Krypto-Portfolios 
mit Hilfe eines KI-Modells (OpenAI API).
"""

import os
import logging
from datetime import datetime
import httpx
from typing import Dict, Any

# Logger konfigurieren
logger = logging.getLogger("OctoAdvisor.analysis")

class PortfolioAnalysisError(Exception):
    """Custom Exception für Portfolio-Analyse Fehler"""
    pass

class OpenAIAPIError(Exception):
    """Custom Exception für OpenAI API Fehler"""
    pass

PORTFOLIO_TEMPLATE = """
AKTUELLES PORTFOLIO:

{% for asset in assets %}
Asset: {{ asset.name }}
Menge: {{ asset.amount }}
{% if asset.eur_value %}
EUR-Wert: €{{ asset.eur_value }}
Aktueller Preis: €{{ asset.price }}
{% if asset.change_24h %}
24h Änderung: {{ asset.change_24h }}%
{% endif %}
{% endif %}

{% endfor %}

GESAMTWERT: €{{ total_value }}
VERFÜGBARES KAPITAL: €{{ free_margin }}

Stand: {{ timestamp }}
"""

def prepare_portfolio_for_analysis(portfolio_data: Dict[str, Any]) -> str:
    """
    Bereitet die Portfolio-Daten für die Analyse vor
    """
    try:
        print("DEBUG - Portfolio Data:", portfolio_data)  # Debug-Ausgabe
        
        # Extrahiere relevante Informationen - KORRIGIERT
        balance_data = portfolio_data.get('balance', {})
        trade_balance_data = portfolio_data.get('trade_balance_eur', {})
        
        # Die echten Daten sind eine Ebene tiefer verschachtelt
        balance = balance_data.get('vol', {})  # Assets sind in 'vol'
        trade_balance_eur = trade_balance_data.get('ZEUR', {})  # EUR-Daten sind in 'ZEUR'
        
        print("DEBUG - Balance:", balance)  # Debug-Ausgabe
        print("DEBUG - Trade Balance EUR:", trade_balance_eur)  # Debug-Ausgabe
        
        # Einfache formatierte Ausgabe
        formatted_data = "AKTUELLES PORTFOLIO:\n\n"
        
        # Gesamtwert aus der TradeBalance API (in EUR)
        try:
            total_value_eur = float(trade_balance_eur.get('eb', 0))
        except (ValueError, TypeError):
            total_value_eur = 0.0
        
        # Assets durchgehen
        asset_count = 0
        for asset, amount in balance.items():
            try:
                # amount ist jetzt direkt ein float/int
                amount = float(amount) if amount else 0.0
                
                # Überspringen, wenn Betrag zu klein
                if amount < 0.0001:
                    continue
                
                formatted_data += f"Asset: {asset}\n"
                formatted_data += f"Menge: {amount:.8f}\n\n"
                asset_count += 1
                
            except Exception as e:
                print(f"DEBUG - Fehler bei Asset {asset}: {e}")
                continue
        
        # Wenn keine Assets gefunden wurden
        if asset_count == 0:
            formatted_data += "Keine Assets mit ausreichender Menge gefunden.\n\n"
        
        # Gesamtwert hinzufügen
        formatted_data += f"GESAMTWERT: €{total_value_eur:.2f}\n"
        
        # Ungebundenes Kapital
        try:
            free_margin = float(trade_balance_eur.get('mf', 0))
        except (ValueError, TypeError):
            free_margin = 0.0
            
        formatted_data += f"VERFÜGBARES KAPITAL: €{free_margin:.2f}\n"
        
        # Zeitstempel
        timestamp = portfolio_data.get('timestamp', datetime.now().isoformat())
        formatted_data += f"\nStand: {timestamp}\n"
        
        print("DEBUG - Formatted Data:", formatted_data)  # Debug-Ausgabe
        
        return formatted_data
        
    except Exception as e:
        error_msg = f"Fehler bei der Aufbereitung der Portfolio-Daten: {str(e)}"
        logger.error(error_msg)
        print("DEBUG - Error:", error_msg)  # Debug-Ausgabe
        return error_msg

def analyze_portfolio(formatted_portfolio: str, prompt_template: str) -> str:
    """
    Analysiert das Portfolio mit Hilfe des OpenAI API.
    
    Args:
        formatted_portfolio (str): Die bereits formatierten Portfolio-Daten
        prompt_template (str): Die Vorlage für den Prompt an das KI-Modell
        
    Returns:
        str: Die Analyse-Ergebnisse vom KI-Modell
    """
    try:
        # API-Key aus Umgebungsvariablen laden
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            raise ValueError("OpenAI API-Key fehlt in der .env-Datei")
        
        # Prompt erstellen, indem die formatierten Daten in die Vorlage eingefügt werden
        prompt = prompt_template.replace("{portfolio_data}", formatted_portfolio)
        
        # API-Anfrage an OpenAI vorbereiten
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}"
        }
        
        # OpenAI-Modell aus .env laden oder Standard verwenden
        model = os.environ.get('OPENAI_MODEL', 'gpt-4-turbo-preview')
        
        # Anfrage-Payload erstellen
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.7,
            "max_tokens": 2000
        }
        
        # API-Anfrage senden
        logger.info(f"Sende Anfrage an OpenAI API mit Modell {model}")
        
        with httpx.Client(timeout=60.0) as client:
            response = client.post(
                "https://api.openai.com/v1/chat/completions",
                headers=headers,
                json=payload
            )
            
            response.raise_for_status()  # Fehler werfen, wenn Status-Code nicht 2xx
            
            # Antwort extrahieren
            response_data = response.json()
            analysis_result = response_data['choices'][0]['message']['content']
            
            logger.info("KI-Analyse erfolgreich abgeschlossen")
            return analysis_result
            
    except httpx.HTTPStatusError as e:
        error_msg = f"OpenAI API Fehler: {e.response.status_code}"
        logger.error(error_msg)
        raise OpenAIAPIError(error_msg) from e
    
    except Exception as e:
        error_msg = f"Unerwarteter Fehler bei der Analyse: {str(e)}"
        logger.error(error_msg)
        raise PortfolioAnalysisError(error_msg) from e

def save_analysis(analysis_result, filename):
    """
    Speichert das Analyseergebnis in einer Textdatei.
    
    Args:
        analysis_result (str): Das zu speichernde Analyseergebnis
        filename (str): Der Pfad zur Zieldatei
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(analysis_result)
        logger.info(f"Analyseergebnis wurde gespeichert: {filename}")
    
    except Exception as e:
        logger.error(f"Fehler beim Speichern des Analyseergebnisses: {str(e)}")
        raise PortfolioAnalysisError(str(e)) from e
