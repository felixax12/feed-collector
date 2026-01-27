from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

if __package__ in (None, ""):
    # Als Skript ausgeführt: Projektwurzel zum Pfad hinzufügen.
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    from feeds import FeedOrchestrator, load_config  # type: ignore
else:
    from . import FeedOrchestrator, load_config


async def _run(config_path: str) -> None:
    config = load_config(config_path)
    orchestrator = FeedOrchestrator(config)
    await orchestrator.start()
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        logging.info("Shutdown angefordert.")
    finally:
        await orchestrator.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Universal Feed Starter")
    parser.add_argument("--config", required=True, help="Pfad zur feeds.yaml")
    args = parser.parse_args()
    asyncio.run(_run(args.config))


if __name__ == "__main__":
    main()
