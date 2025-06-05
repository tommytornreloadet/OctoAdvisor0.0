#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OctoAdvisor - Kraken-Modul

Dieses Modul ist verantwortlich für die Interaktion mit der Kraken API.
Es ruft Portfolio-Daten ab und speichert sie im JSON-Format.
"""

import os
import json
import logging
import time
from datetime import datetime
import krakenex
from pykrakenapi import KrakenAPI

# Logger konfigurieren
logger = logging.getLogger("OctoAdvisor.kraken")

def connect_to_kraken():
    """
    Stellt eine Verbindung zur Kraken API her.
    
    Returns:
        KrakenAPI: Eine Instanz der Kraken API
    """
    try:
        # API-Key und Secret aus Umgebungsvariablen laden
        api_key = os.environ.get('KRAKEN_API_KEY')
        api_secret = os.environ.get('KRAKEN_API_SECRET')
        
        if not api_key or not api_secret:
            raise ValueError("Kraken API-Zugangsdaten fehlen in der .env-Datei")
        
        # Kraken API Client initialisieren
        api = krakenex.API(key=api_key, secret=api_secret)
        kraken = KrakenAPI(api)
        
        logger.info("Verbindung zur Kraken API hergestellt")
        return kraken
    
    except Exception as e:
        logger.error(f"Fehler beim Verbinden zur Kraken API: {str(e)}")
        raise

def get_ticker_data(kraken_api, assets):
    """
    Ruft aktuelle Kursdaten für die angegebenen Assets ab.
    
    Args:
        kraken_api (KrakenAPI): Kraken API Instanz
        assets (list): Liste der Asset-Paare (z.B. ["XXBTZUSD", "XETHZUSD"])
    
    Returns:
        dict: Kursdaten für die angegebenen Assets
    """
    try:
        ticker_data = {}
        
        # In Gruppen von maximal 10 Assets anfragen (API-Limit)
        for i in range(0, len(assets), 10):
            batch = assets[i:i+10]
            pairs = ",".join(batch)
            response = kraken_api.query_public('Ticker', {'pair': pairs})
            
            if 'error' in response and response['error']:
                logger.warning(f"API-Warnung bei Ticker-Anfrage: {response['error']}")
            
            if 'result' in response:
                ticker_data.update(response['result'])
            
            # Rate-Limiting beachten
            time.sleep(1)
        
        return ticker_data
    
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Ticker-Daten: {str(e)}")
        return {}

def get_portfolio():
    """
    Ruft das aktuelle Portfolio und relevante Marktdaten von Kraken ab.
    
    Returns:
        dict: Ein Dictionary mit Portfolio-Daten und Marktinformationen
    """
    try:
        kraken_api = connect_to_kraken()
        
        # 1. Kontostand abrufen
        balance = kraken_api.get_account_balance()
        time.sleep(1)  # 1 Sekunde Pause
        
        # 2. Offene Orders abrufen
        open_orders = kraken_api.get_open_orders()
        
        # 3. Handelshistorie der letzten 50 Trades abrufen
        trades_history = kraken_api.get_trades_history()
        
        # 4. Zusätzlich die Extended Balance Information abrufen (in EUR)
        trade_balance_eur = kraken_api.get_trade_balance(asset='ZEUR')
        time.sleep(1)  # 1 Sekunde Pause
        
        # 5. Ticker-Daten für alle gehaltenen Assets abrufen
        # Zunächst alle Assets mit einem Bestand > 0 finden
        assets_with_balance = [asset for asset, amount in balance.to_dict().items() 
                              if float(amount) > 0.0001]
        
        # Asset-Paare für EUR erstellen (z.B. XXBTZEUR, XETHZEUR)
        pairs = []
        for asset in assets_with_balance:
            # Überspringen von Fiat-Währungen
            if asset in ['ZUSD', 'ZEUR']:
                continue
            pairs.append(f"{asset}ZEUR")
        
        # Ticker-Daten abrufen
        ticker_data = get_ticker_data(kraken_api.api, pairs)
        
        # Portfolio-Daten zusammenstellen
        portfolio_data = {
            "timestamp": datetime.now().isoformat(),
            "balance": balance.to_dict(),
            "trade_balance_eur": trade_balance_eur['result'] if 'result' in trade_balance_eur else {},
            "open_orders": open_orders.to_dict() if not open_orders.empty else {},
            "trades_history": trades_history[0].to_dict() if not trades_history[0].empty else {},
            "ticker_data": ticker_data
        }
        
        return portfolio_data
    
    except Exception as e:
        logger.error(f"Fehler beim Abrufen des Portfolios: {str(e)}")
        raise

def save_portfolio(portfolio_data, filename):
    """
    Speichert die Portfolio-Daten als JSON-Datei.
    
    Args:
        portfolio_data (dict): Die zu speichernden Portfolio-Daten
        filename (str): Der Pfad zur Zieldatei
    """
    try:
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(portfolio_data, f, indent=2, ensure_ascii=False)
        logger.info(f"Portfolio-Daten wurden gespeichert: {filename}")
    
    except Exception as e:
        logger.error(f"Fehler beim Speichern der Portfolio-Daten: {str(e)}")
        raise

def get_portfolio_simple():
    """Vereinfachte Version ohne Trade History für Tests"""
    try:
        kraken_api = connect_to_kraken()
        
        balance = kraken_api.get_account_balance()
        time.sleep(2)  # Längere Pause zwischen Aufrufen
        
        trade_balance_eur = kraken_api.get_trade_balance(asset='ZEUR')
        time.sleep(2)
        
        return {
            'balance': balance.to_dict(),
            'trade_balance_eur': trade_balance_eur.to_dict() if hasattr(trade_balance_eur, 'to_dict') else trade_balance_eur,
            'ticker_data': {},  # Leer lassen für Test
            'timestamp': datetime.now().isoformat()
        }
    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Portfolio-Daten: {str(e)}")
        raise 