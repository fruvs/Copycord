import os
import sys
import logging

logger = logging.getLogger(__name__)

class Config:
    def __init__(self):
        # ─── Server-side settings ─────────────────────────────────
        self.SERVER_TOKEN    = os.getenv("SERVER_TOKEN")
        self.CLONE_GUILD_ID  = os.getenv("CLONE_GUILD_ID", "0")
        self.DB_PATH         = os.getenv("DB_PATH", "/data/data.db")
        self.SERVER_WS_HOST  = os.getenv("SERVER_WS_HOST", "server")
        self.SERVER_WS_PORT  = int(os.getenv("SERVER_WS_PORT", "8765"))
        self.SERVER_WS_URL   = os.getenv(
            "WS_SERVER_URL",
            f"ws://{self.SERVER_WS_HOST}:{self.SERVER_WS_PORT}"
        )
        self.DELETE_CHANNELS = os.getenv("DELETE_CHANNELS", "false")\
            .lower() in ("1", "true", "yes")
        self.DELETE_THREADS  = os.getenv("DELETE_THREADS",  "false")\
            .lower() in ("1", "true", "yes")
            
        raw = os.getenv("COMMAND_USERS", "")
        self.COMMAND_USERS    = [int(u) for u in raw.split(",") if u.strip()]
        
        # ─── Client-side settings ─────────────────────────────────
        self.CLIENT_TOKEN    = os.getenv("CLIENT_TOKEN")
        self.HOST_GUILD_ID   = os.getenv("HOST_GUILD_ID", "0")
        self.CLIENT_WS_HOST  = os.getenv("CLIENT_WS_HOST", "client")
        self.CLIENT_WS_PORT  = int(os.getenv("CLIENT_WS_PORT", "8766"))
        self.CLIENT_WS_URL   = os.getenv(
            "WS_CLIENT_URL",
            f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )
        self.SYNC_INTERVAL_SECONDS = int(
            os.getenv("SYNC_INTERVAL_SECONDS", "3600")
        )

        # ─── VALIDATION ─────────────────────────────────────────────────
        for name in ("SERVER_TOKEN", "CLIENT_TOKEN"):
            if not getattr(self, name):
                logger.error(f"Missing required environment variable {name}")
                sys.exit(1)

        for name in ("HOST_GUILD_ID", "CLONE_GUILD_ID"):
            raw = getattr(self, name)
            if raw is None:
                logger.error(f"Missing required Discord guild ID env var: {name}")
                sys.exit(1)
            try:
                val = int(raw)
            except (TypeError, ValueError):
                logger.error(f"Discord guild ID {name} must be an integer (got {raw!r}); aborting.")
                sys.exit(1)
            if val <= 0:
                logger.error(
                    f"Discord guild ID {name} must be a positive integer (got {val}); shutting down."
                )
                sys.exit(1)
            setattr(self, name, val)