"""
Modulares Feed-Paket mit einheitlichem Datenformat.

Dieses Paket deklariert nur die öffentlich verfügbaren Einstiegspunkte
für den Orchestrator und die Konfig-Ladefunktion.
"""

from .config import AppConfig, load_config
from .orchestrator import FeedOrchestrator

__all__ = ["AppConfig", "FeedOrchestrator", "load_config"]
