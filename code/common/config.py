import os

class Config:
    def __init__(self):
        # ─── Server-side settings ─────────────────────────────────
        self.SERVER_TOKEN    = os.getenv("SERVER_TOKEN")
        self.CLONE_GUILD_ID  = int(os.getenv("CLONE_GUILD_ID", "0"))
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
        self.HOST_GUILD_ID = int(os.getenv("HOST_GUILD_ID", "0"))
        self.CLIENT_WS_HOST  = os.getenv("CLIENT_WS_HOST", "client")
        self.CLIENT_WS_PORT  = int(os.getenv("CLIENT_WS_PORT", "8766"))
        self.CLIENT_WS_URL   = os.getenv(
            "WS_CLIENT_URL",
            f"ws://{self.CLIENT_WS_HOST}:{self.CLIENT_WS_PORT}"
        )
        self.SITEMAP_INTERVAL_SECONDS = int(
            os.getenv("SITEMAP_INTERVAL_SECONDS", "3600")
        )

