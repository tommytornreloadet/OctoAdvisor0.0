#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
OctoAdvisor - Telegram-Bot-Modul

Dieses Modul ist verantwortlich für den Versand von Nachrichten
über einen Telegram-Bot.
"""

import os
import logging
import httpx
from typing import List
from config import Config
import time

# Logger konfigurieren
logger = logging.getLogger("OctoAdvisor.telegram")

def split_message(message: str, max_length: int = 4000) -> List[str]:
    """Teilt lange Nachrichten in kleinere Teile auf."""
    if len(message) <= max_length:
        return [message]
    
    parts = []
    while message:
        if len(message) <= max_length:
            parts.append(message)
            break
        
        split_point = message[:max_length].rfind('\n')
        if split_point == -1:
            split_point = max_length
        
        parts.append(message[:split_point])
        message = message[split_point:].lstrip()
    
    return parts

async def send_message_async(message: str, config: Config) -> bool:
    """Asynchrone Version für bessere Performance."""
    try:
        url = f"https://api.telegram.org/bot{config.TELEGRAM_BOT_TOKEN}/sendMessage"
        message_parts = split_message(message, config.MAX_TELEGRAM_MESSAGE_LENGTH)
        
        async with httpx.AsyncClient(timeout=config.API_TIMEOUT) as client:
            for part in message_parts:
                payload = {
                    "chat_id": config.TELEGRAM_CHAT_ID,
                    "text": part,
                    "parse_mode": "Markdown"
                }
                response = await client.post(url, json=payload)
                response.raise_for_status()
        
        return True
    except Exception as e:
        logger.error(f"Telegram Fehler: {str(e)}")
        return False

def send_document(file_path, caption=None):
    """
    Sendet eine Datei über den Telegram-Bot.
    
    Args:
        file_path (str): Pfad zur zu sendenden Datei
        caption (str, optional): Beschreibung der Datei
    
    Returns:
        bool: True bei Erfolg, False bei Fehler
    """
    try:
        # Telegram Bot Token und Chat-ID aus Umgebungsvariablen laden
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            raise ValueError("Telegram Bot Token oder Chat-ID fehlen in der .env-Datei")
        
        # Telegram API URL
        url = f"https://api.telegram.org/bot{bot_token}/sendDocument"
        
        # Datei überprüfen
        if not os.path.exists(file_path):
            logger.error(f"Die Datei {file_path} existiert nicht")
            return False
        
        # Datei senden
        with open(file_path, 'rb') as document:
            files = {'document': document}
            data = {'chat_id': chat_id}
            
            if caption:
                data['caption'] = caption
            
            with httpx.Client(timeout=60.0) as client:
                response = client.post(url, data=data, files=files)
                
                if response.status_code != 200:
                    logger.error(f"Telegram API-Fehler: {response.status_code} - {response.text}")
                    return False
                
                logger.info(f"Datei {file_path} erfolgreich über Telegram gesendet")
                return True
    
    except Exception as e:
        logger.error(f"Fehler beim Senden der Datei über Telegram: {str(e)}")
        return False

def send_message(message: str) -> bool:
    """
    Sendet eine Nachricht über den Telegram Bot.
    
    Args:
        message (str): Die zu sendende Nachricht
        
    Returns:
        bool: True wenn erfolgreich, False bei Fehler
    """
    try:
        bot_token = os.environ.get('TELEGRAM_BOT_TOKEN')
        chat_id = os.environ.get('TELEGRAM_CHAT_ID')
        
        if not bot_token or not chat_id:
            logger.error("Telegram Bot Token oder Chat ID fehlen")
            return False
        
        url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
        
        # Nachricht in Teile aufteilen falls zu lang
        message_parts = split_message(message, 4000)
        
        for part in message_parts:
            payload = {
                "chat_id": chat_id,
                "text": part,
                "parse_mode": "Markdown"
            }
            
            response = httpx.post(url, json=payload, timeout=30)
            response.raise_for_status()
            
            # Kurze Pause zwischen Nachrichten
            time.sleep(0.5)
        
        logger.info("Telegram-Nachricht erfolgreich gesendet")
        return True
        
    except Exception as e:
        logger.error(f"Fehler beim Senden der Telegram-Nachricht: {str(e)}")
        return False 