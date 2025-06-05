#!/usr/bin/env python3
"""
Interaktives CLI-Skript data_fetcher.py zum Aktualisieren von OHLCV-Feather-Daten via CCXT.

Features:
- Banner: data_fetcher
- Interaktive Auswahl von Exchange (Binance, Kraken) per Nummern-Menü
- Anzeige aller vorhandenen Daten und Zeiträume
- Auswahl von einer oder mehreren Timeframes (oder alle)
- Anzeige und Mehrfach-Auswahl bestehender Paare (.feather)
- Neuerstellung eines Paares
- Speicherung als Feather und optional CSV
- Zu jeder Frage genau eine Leerzeile Abstand
- Futuristisch-türkise Terminal-Formatierung
- Jederzeitige Möglichkeit zum Beenden mit 'q'
"""

import logging
import sys
from pathlib import Path
from time import sleep

import ccxt
import click
import pandas as pd

# --- Logging konfigurieren ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# --- Style Helper ---
def print_banner():
    """Futuristisches ASCII-Banner"""
    banner = """
    ╔╦╗╔═╗╔╦╗╔═╗   ╔═╗╔═╗╔╦╗╔═╗╦ ╦╔═╗╦═╗
     ║║╠═╣ ║ ╠═╣───╠╣ ║╣  ║ ║  ╠═╣║╣ ╠╦╝
    ═╩╝╩ ╩ ╩ ╩ ╩   ╚  ╚═╝ ╩ ╚═╝╩ ╩╚═╝╩╚═
    """
    click.echo()
    click.secho(banner, fg="cyan", bold=True)
    click.echo()


def print_header(text: str):
    """Formatierte Abschnitts-Überschrift mit Leerzeilen"""
    click.echo()
    click.secho(text, fg="cyan")
    click.echo()


def ask_confirm(message: str) -> bool:
    """Yes/No-Abfrage mit kleinem '[y/n]' Prompt ohne zusätzliche Hinweise"""
    return click.confirm(message, default=False)


def ask_prompt(message: str, **kwargs) -> str:
    click.echo()
    ans = click.prompt(message, **kwargs)
    return ans


def ask_choice(message: str, options: list[str], allow_custom: bool = False) -> str:
    """
    Einfaches Nummern-Menü. Gibt ausgewählten Wert. 'q' beendet.
    allow_custom: fügt Auswahl 0 für Freitext hinzu.
    """
    while True:
        for idx, opt in enumerate(options, start=1):
            click.echo(f"  {idx}) {opt}")
        if allow_custom:
            click.echo("  0) Anderen eingeben")
        click.echo("  q) Beenden")
        click.echo()
        choice = click.prompt(message, default="1")
        if choice.lower() == "q":
            click.echo("Beendet.")
            sys.exit(0)
        if choice.isdigit():
            idx = int(choice)
            if idx == 0 and allow_custom:
                return ask_prompt("Gib Wert ein")
            if 1 <= idx <= len(options):
                return options[idx - 1]
        click.secho("Ungültige Auswahl, bitte erneut.", fg="red")


def ask_multi_choice(message: str, options: list[str]) -> list[str]:
    """
    Mehrfach-Auswahl per Komma oder 'a' für alle. 'q' beendet.
    0 startet Freitext-Paar-Eingabe.
    """
    while True:
        for idx, opt in enumerate(options, start=1):
            click.echo(f"  {idx}) {opt}")
        click.echo("  0) Neues Paar hinzufügen")
        click.echo("  a) Alle auswählen")
        click.echo("  q) Beenden")
        click.echo()
        sel = click.prompt(message, default="a")
        if sel.lower() == "q":
            click.echo("Beendet.")
            sys.exit(0)
        if sel.lower() == "a":
            return options
        indices = []
        for part in sel.split(","):
            p = part.strip()
            if p == "0":
                indices.append(0)
            elif p.isdigit() and 1 <= int(p) <= len(options):
                indices.append(int(p))
        if not indices:
            click.secho("Ungültige Auswahl, bitte erneut.", fg="red")
            continue
        result = []
        for i in indices:
            if i == 0:
                result.append(ask_prompt("Gib neues Paar ein (z.B. BTC/EUR)"))
            else:
                result.append(options[i - 1])
        return result


# --- Datenfunktionen ---


def ensure_dir(path: Path):
    path.mkdir(parents=True, exist_ok=True)


def parse_file_info(file_path: Path):
    """Extrahiert Paar und Timeframe aus Dateinamen."""
    stem = file_path.stem
    if "-" not in stem:
        return None, None
    symbol, timeframe = stem.rsplit("-", 1)
    return symbol.replace("_", "/"), timeframe


def load_existing_feather(filepath: Path) -> pd.DataFrame:
    df = pd.read_feather(filepath)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], utc=True)
        df.set_index("date", inplace=True)
    elif "ts" in df.columns:
        df["date"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
        df.set_index("date", inplace=True)
    else:
        df.index = pd.to_datetime(df.index, utc=True)
    return df


def list_all_data(data_root: Path):
    for exchange_dir in sorted(data_root.iterdir()):
        if not exchange_dir.is_dir():
            continue
        print_header(f"=== Exchange: {exchange_dir.name} ===")
        files = list(exchange_dir.glob("*.feather"))
        if not files:
            click.echo("(keine Dateien)")
            continue
        for f in files:
            pair, tf = parse_file_info(f)
            try:
                df = load_existing_feather(f)
                if df.empty:
                    rng = "keine Daten"
                else:
                    rng = f"{df.index.min().strftime('%Y-%m-%d')} bis {df.index.max().strftime('%Y-%m-%d')}"
                click.echo(f"{pair} {tf}: {rng} ({len(df)} Kerzen)")
            except Exception as e:
                click.secho(f"Fehler beim Lesen {f.name}: {e}", fg="red")


def fetch_ohlcv(
    exchange: ccxt.Exchange,
    symbol: str,
    timeframe: str,
    since_ms: int,
    limit: int,
    rate_limit: float,
) -> list:
    all_ohlcv = []
    now_ms = exchange.milliseconds()
    while since_ms < now_ms:
        try:
            chunk = exchange.fetch_ohlcv(symbol, timeframe, since=since_ms, limit=limit)
        except Exception as e:
            logger.warning(f"Fehler beim Fetch für {symbol}: {e} – retry in 5s")
            sleep(5)
            continue
        if not chunk:
            break
        ts_human = pd.to_datetime(since_ms, unit="ms", utc=True)
        logger.info(f"Fetched {len(chunk)} Kerzen für {symbol} ab {ts_human}")
        all_ohlcv.extend(chunk)
        since_ms = chunk[-1][0] + 1
        sleep(rate_limit)
    logger.info(f"Insgesamt {len(all_ohlcv)} Kerzen geladen für {symbol}")
    return all_ohlcv


def update_pair(
    exchange_obj,
    data_dir: Path,
    pair: str,
    timeframe: str,
    since_str: str,
    limit: int,
    rate_limit: float,
    csv_enabled: bool = False,
):
    logger.info(f"→ Update {pair} {timeframe}")
    fname = f"{pair.replace('/', '_')}-{timeframe}"
    feather_path = data_dir / f"{fname}.feather"
    csv_path = data_dir / f"{fname}.csv"
    if feather_path.exists():
        df_old = load_existing_feather(feather_path)
        since_ms = int(df_old.index.max().timestamp() * 1000) + 1
    else:
        df_old = pd.DataFrame(columns=["open", "high", "low", "close", "volume"])
        since_ms = exchange_obj.parse8601(since_str)
    ohlcv = fetch_ohlcv(exchange_obj, pair, timeframe, since_ms, limit, rate_limit)
    if not ohlcv:
        logger.info("– keine neuen Daten")
        return
    df_new = pd.DataFrame(
        ohlcv, columns=["ts", "open", "high", "low", "close", "volume"]
    )
    df_new["date"] = pd.to_datetime(df_new["ts"], unit="ms", utc=True)
    df_new.set_index("date", inplace=True)
    df_new = df_new[["open", "high", "low", "close", "volume"]]
    if not df_old.empty:
        df = pd.concat([df_old, df_new])
        df = df[~df.index.duplicated(keep="last")]
        df.sort_index(inplace=True)
    else:
        df = df_new
    ensure_dir(data_dir)
    df.reset_index().to_feather(feather_path)
    logger.info(f"– Feather gespeichert: {feather_path.name}")
    if csv_enabled:
        df.to_csv(csv_path, float_format="%.8f")
        logger.info(f"– CSV gespeichert: {csv_path.name}")


def show_main_menu():
    """Hauptmenü anzeigen und Auswahl zurückgeben"""
    print_header("Hauptmenü")
    options = [
        "Anzeige",  # Vorhandene Daten anzeigen
        "Update",  # Bestehende Daten aktualisieren
        "Download",  # Neue Paare hinzufügen
        "Löschen",  # Paare löschen
        "Automode",  # Automatische Aktualisierung
    ]
    choice = ask_choice("Wähle eine Option", options)
    return choice


def get_dir_size(path: Path) -> int:
    """Berechnet die Größe eines Verzeichnisses in Bytes"""
    total_size = 0
    for item in path.glob("**/*"):
        if item.is_file():
            total_size += item.stat().st_size
    return total_size


def format_size(size_bytes: int) -> str:
    """Formatiert Bytes in lesbare Größe (KB, MB, GB)"""
    if size_bytes < 1024:
        return f"{size_bytes} Bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"


def list_by_timeframe(data_root: Path):
    """Zeigt alle Daten sortiert nach Timeframes an"""
    # Speichere alle Paare nach Timeframe
    timeframe_data = {}
    exchange_files = {}

    for exchange_dir in sorted(data_root.iterdir()):
        if not exchange_dir.is_dir():
            continue

        files = list(exchange_dir.glob("*.feather"))
        exchange_files[exchange_dir.name] = files

        for f in files:
            pair, tf = parse_file_info(f)
            if not pair or not tf:
                continue

            if tf not in timeframe_data:
                timeframe_data[tf] = {}

            if exchange_dir.name not in timeframe_data[tf]:
                timeframe_data[tf][exchange_dir.name] = []

            timeframe_data[tf][exchange_dir.name].append(pair)

    # Für jeden Timeframe
    for tf in sorted(timeframe_data.keys()):
        print_header(f"Timeframe: {tf}")

        for exchange, pairs in sorted(timeframe_data[tf].items()):
            click.echo(f"  Exchange: {exchange} ({len(pairs)} Paare)")
            for pair in sorted(pairs):
                click.echo(f"    - {pair}")


def list_by_pair(data_root: Path):
    """Zeigt alle Daten sortiert nach Paaren an"""
    # Speichere alle Timeframes nach Paar
    pair_data = {}

    for exchange_dir in sorted(data_root.iterdir()):
        if not exchange_dir.is_dir():
            continue

        files = list(exchange_dir.glob("*.feather"))

        for f in files:
            pair, tf = parse_file_info(f)
            if not pair or not tf:
                continue

            if pair not in pair_data:
                pair_data[pair] = {}

            if exchange_dir.name not in pair_data[pair]:
                pair_data[pair][exchange_dir.name] = []

            pair_data[pair][exchange_dir.name].append(tf)

    # Sortiere nach Paaren
    for pair in sorted(pair_data.keys()):
        print_header(f"Pair: {pair}")

        for exchange, timeframes in sorted(pair_data[pair].items()):
            click.echo(f"  Exchange: {exchange}")
            for tf in sorted(timeframes):
                # Für jedes Pair/Exchange/Timeframe die Datendetails laden
                file_path = exchange_dir / f"{pair.replace('/', '_')}-{tf}.feather"
                try:
                    df = load_existing_feather(file_path)
                    if df.empty:
                        details = "keine Daten"
                    else:
                        date_range = f"{df.index.min().strftime('%Y-%m-%d')} bis {df.index.max().strftime('%Y-%m-%d')}"
                        details = f"{date_range} ({len(df)} Kerzen)"
                except Exception:
                    details = "Fehler beim Laden"

                click.echo(f"    - {tf}: {details}")


def run_data_display(data_root: Path):
    """Zeigt Daten mit Auswahlmöglichkeit des Display-Formats"""
    print_header("Daten anzeigen")
    options = ["Standard", "Sortiert nach Timeframe", "Sortiert nach Pair"]
    choice = ask_choice("Wähle ein Anzeigeformat", options)

    # Zeige Gesamtgröße
    total_size = get_dir_size(data_root)
    click.secho(f"Datenverzeichnisgröße: {format_size(total_size)}", fg="green")

    if choice == "Standard":
        list_all_data(data_root)
    elif choice == "Sortiert nach Timeframe":
        list_by_timeframe(data_root)
    else:
        list_by_pair(data_root)


@click.command()
@click.option(
    "--since",
    default="2012-01-01T00:00:00Z",
    help="Start-Datum ISO, falls keine Daten existieren.",
)
@click.option("--limit", default=1000, help="Maximal Candles pro Anfrage.")
@click.option("--rate-limit", default=0.2, help="Pause zwischen Anfragen in Sekunden.")
def main(since, limit, rate_limit):
    print_banner()

    # Pfade
    script_dir = Path(__file__).parent
    data_root = script_dir / "data"
    
    # Neuen OHLCV-Unterordner hinzufügen
    ohlcv_root = data_root / "ohlcv"

    while True:
        choice = show_main_menu()

        if choice == "Anzeige":
            run_data_display(ohlcv_root)
        elif choice == "Update":
            run_data_update(ohlcv_root, since, limit, rate_limit)
        elif choice == "Download":
            run_data_download(ohlcv_root, since, limit, rate_limit)
        elif choice == "Löschen":
            run_data_delete(ohlcv_root)
        elif choice == "Automode":
            run_auto_update(ohlcv_root, since, limit, rate_limit)


def run_data_update(data_root, since, limit, rate_limit):
    """Update bestehender Daten - nur existierende Paare aktualisieren"""
    # Exchange-Auswahl
    print_header("Wähle Exchange")
    exchange = ask_choice("Deine Auswahl", ["binance", "kraken"])
    data_dir = data_root / exchange

    # Timeframe-Auswahl
    files = list(data_dir.glob("*.feather"))
    tfs = sorted({parse_file_info(f)[1] for f in files if parse_file_info(f)[1]})
    print_header("Verfügbare Timeframes")
    timeframes = ask_multi_choice(
        "Wähle Index(es) der Timeframes oder 'a' für alle", tfs
    )

    # Exchange-Objekt
    exchange_obj = getattr(ccxt, exchange)(
        {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    )

    # Durch alle Timeframes iterieren
    for timeframe in timeframes:
        print_header(f"Zeitframe: {timeframe}")

        # Paar-Auswahl für diesen TF - nur existierende anzeigen
        files_tf = [f for f in files if parse_file_info(f)[1] == timeframe]
        existing = [parse_file_info(f)[0] for f in files_tf]

        if not existing:
            click.echo("Keine existierenden Paare für diesen Timeframe gefunden.")
            continue

        pairs = ask_multi_choice(
            "Wähle Index(es) der Paare oder 'a' für alle", existing
        )

        for pair in pairs:
            try:
                update_pair(
                    exchange_obj,
                    data_dir,
                    pair,
                    timeframe,
                    since,
                    limit,
                    rate_limit,
                    False,
                )
            except Exception as e:
                logger.error(f"Fehler beim Update von {pair}: {e}")


def fetch_top_pairs(exchange_obj, quote_currency="EUR", limit=10):
    """Hole die Top-Handelspaare nach 24h-Volumen"""
    try:
        # Alle Ticker holen
        tickers = exchange_obj.fetch_tickers()

        # Nach Volume sortieren und Top-Paare zurückgeben
        pairs = []

        for symbol, ticker in tickers.items():
            # Nur Paare mit dem gewünschten Quote-Asset (z.B. EUR)
            if symbol.endswith(f"/{quote_currency}"):
                if "quoteVolume" in ticker and ticker["quoteVolume"]:
                    pairs.append((symbol, ticker["quoteVolume"]))
                elif "volume" in ticker and ticker["volume"]:
                    pairs.append((symbol, ticker["volume"]))

        # Nach Volumen sortieren
        pairs.sort(key=lambda x: x[1], reverse=True)

        # Top X Paare zurückgeben
        return [pair[0] for pair in pairs[:limit]]
    except Exception as e:
        logger.error(f"Fehler beim Laden der Top-Pairs: {e}")
        return []


def get_timerange_option():
    """Zeitraum vor aktuellem Datum auswählen"""
    print_header("Zeitraum auswählen")
    options = ["1 Tag", "1 Monat", "1 Jahr", "Max (alle verfügbaren Daten)"]
    choice = ask_choice("Wähle den Zeitraum für den Download", options)

    # Aktuelles Datum und Zeit
    now = pd.Timestamp.now(tz="UTC")

    if choice == "1 Tag":
        # 1 Tag zurück
        since = now - pd.Timedelta(days=1)
    elif choice == "1 Monat":
        # 1 Monat zurück
        since = now - pd.Timedelta(days=30)
    elif choice == "1 Jahr":
        # 1 Jahr zurück
        since = now - pd.Timedelta(days=365)
    else:
        # Max (default Startdatum)
        return "2012-01-01T00:00:00Z"

    # Konvertieren in ISO-Format
    return since.strftime("%Y-%m-%dT%H:%M:%SZ")


def run_data_download(data_root, since, limit, rate_limit):
    """Download neuer Daten - neue Paare hinzufügen"""
    # Exchange-Auswahl
    print_header("Wähle Exchange")
    exchange = ask_choice("Deine Auswahl", ["binance", "kraken"])
    data_dir = data_root / exchange
    ensure_dir(data_dir)

    # Exchange-Objekt
    exchange_obj = getattr(ccxt, exchange)(
        {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    )

    # Timeframe-Auswahl
    print_header("Timeframe auswählen")
    common_tfs = ["1m", "5m", "15m", "1h", "4h", "1d"]
    timeframe = ask_choice("Wähle einen Timeframe", common_tfs, allow_custom=True)

    # Zeitraum-Auswahl (neu)
    custom_since = get_timerange_option()

    # Top-Paare anzeigen
    if exchange == "binance":
        top_pairs = fetch_top_pairs(exchange_obj)
        if top_pairs:
            print_header("Top 10 Paare nach 24h-Volumen (Binance)")
            for i, pair in enumerate(top_pairs, 1):
                click.echo(f"  {i}. {pair}")
            click.echo()

    # Paar-Eingabe
    print_header("Paare eingeben")
    click.echo("Gib die Paare ein, die du herunterladen möchtest.")
    click.echo("Beispiele: BTC/EUR, ETH/EUR, SOL/USD")

    pairs_input = ask_prompt("Paare (mit Komma getrennt)")
    pairs = [p.strip() for p in pairs_input.split(",") if p.strip()]

    if not pairs:
        click.secho("Keine Paare angegeben, Abbruch.", fg="red")
        return

    # Download starten
    print_header(f"Download von {len(pairs)} Paaren im Timeframe {timeframe}")
    start_date = pd.to_datetime(custom_since, utc=True).strftime("%Y-%m-%d")
    click.echo(f"Daten ab: {start_date}")

    for pair in pairs:
        try:
            click.echo(f"\nDownload von {pair}...")
            update_pair(
                exchange_obj,
                data_dir,
                pair,
                timeframe,
                custom_since,  # Verwende ausgewählten Zeitraum
                limit,
                rate_limit,
                False,
            )
        except Exception as e:
            logger.error(f"Fehler beim Download von {pair}: {e}")


def run_auto_update(data_root, since, limit, rate_limit):
    """Automatischer Update aller vorhandenen Paare einer Exchange"""
    # Exchange-Auswahl
    print_header("Wähle Exchange für Auto-Update")
    exchange = ask_choice("Deine Auswahl", ["binance", "kraken"])
    data_dir = data_root / exchange

    if not data_dir.exists():
        click.secho(f"Exchange-Verzeichnis nicht gefunden: {data_dir}", fg="red")
        return

    # Exchange-Objekt
    exchange_obj = getattr(ccxt, exchange)(
        {"enableRateLimit": True, "options": {"defaultType": "spot"}}
    )

    # Alle Dateien im Verzeichnis finden
    files = list(data_dir.glob("*.feather"))
    if not files:
        click.secho(f"Keine Dateien für {exchange} gefunden.", fg="red")
        return

    # Organisieren nach Timeframes und Paaren
    timeframe_pairs = {}
    for f in files:
        pair, tf = parse_file_info(f)
        if not pair or not tf:
            continue
        if tf not in timeframe_pairs:
            timeframe_pairs[tf] = []
        timeframe_pairs[tf].append(pair)

    # Statistik anzeigen
    total_pairs = sum(len(pairs) for pairs in timeframe_pairs.values())
    print_header(f"Auto-Update für {exchange}")
    click.echo(
        f"Gefunden: {len(timeframe_pairs)} Timeframes mit insgesamt {total_pairs} Paaren"
    )
    for tf, pairs in timeframe_pairs.items():
        click.echo(f"  {tf}: {len(pairs)} Paare")

    if not ask_confirm("Update starten?"):
        return

    # Für jeden Timeframe alle Paare aktualisieren
    for tf, pairs in timeframe_pairs.items():
        print_header(f"Update für Timeframe {tf}")
        click.echo(f"Aktualisiere {len(pairs)} Paare...")

        for pair in pairs:
            try:
                update_pair(
                    exchange_obj,
                    data_dir,
                    pair,
                    tf,
                    since,
                    limit,
                    rate_limit,
                    False,
                )
            except Exception as e:
                logger.error(f"Fehler beim Update von {pair}/{tf}: {e}")

    print_header("Auto-Update abgeschlossen")
    click.echo(f"Alle {total_pairs} Paare wurden aktualisiert.")


def run_data_delete(data_root):
    """Löschen von Paaren"""
    # Exchange-Auswahl
    print_header("Wähle Exchange")
    exchange = ask_choice("Deine Auswahl", ["binance", "kraken"])
    data_dir = data_root / exchange

    if not data_dir.exists():
        click.secho(f"Exchange-Verzeichnis nicht gefunden: {data_dir}", fg="red")
        return

    # Timeframe-Auswahl
    files = list(data_dir.glob("*.feather"))
    if not files:
        click.secho(f"Keine Dateien für {exchange} gefunden.", fg="red")
        return

    tfs = sorted({parse_file_info(f)[1] for f in files if parse_file_info(f)[1]})
    print_header("Verfügbare Timeframes")
    timeframe = ask_choice("Wähle einen Timeframe", tfs)

    # Paar-Auswahl für diesen TF
    files_tf = [f for f in files if parse_file_info(f)[1] == timeframe]
    existing = [parse_file_info(f)[0] for f in files_tf]

    if not existing:
        click.secho(f"Keine Paare für Timeframe {timeframe} gefunden.", fg="red")
        return

    print_header(f"Paare zum Löschen für {timeframe}")
    pairs_to_delete = ask_multi_choice(
        "Wähle zu löschende Paare oder 'a' für alle", existing
    )

    if not pairs_to_delete:
        click.secho("Keine Paare ausgewählt, Abbruch.", fg="red")
        return

    # Bestätigung vor dem Löschen
    click.echo()
    click.secho("Folgende Paare werden gelöscht:", fg="red")
    for pair in pairs_to_delete:
        click.echo(f"  - {pair} ({timeframe})")

    if not ask_confirm(
        "Bist du sicher? Diese Aktion kann nicht rückgängig gemacht werden!"
    ):
        click.echo("Abbruch, keine Dateien gelöscht.")
        return

    # Löschen
    deleted_count = 0
    for pair in pairs_to_delete:
        fname = f"{pair.replace('/', '_')}-{timeframe}"
        feather_path = data_dir / f"{fname}.feather"
        csv_path = data_dir / f"{fname}.csv"

        if feather_path.exists():
            feather_path.unlink()
            deleted_count += 1
            click.echo(f"Gelöscht: {feather_path.name}")

        if csv_path.exists():
            csv_path.unlink()
            click.echo(f"Gelöscht: {csv_path.name}")

    print_header("Löschvorgang abgeschlossen")
    click.echo(f"{deleted_count} Dateien wurden gelöscht.")


if __name__ == "__main__":
    main()
