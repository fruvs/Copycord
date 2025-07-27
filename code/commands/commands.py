# commands/commands.py
import discord
from discord.ext import commands
from discord import Option
from discord import errors as discord_errors
from datetime import datetime, timezone
import time
import logging
from common.config import Config
from common.db import DBManager

logger = logging.getLogger("commands")

config = Config()
GUILD_ID = config.CLONE_GUILD_ID

class CloneCommands(commands.Cog):
    """
    Collection of slash commands for the Clone bot, restricted to allowed users.
    """
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.db = DBManager(config.DB_PATH)
        self.start_time = time.time()
        self.allowed_users = getattr(config, 'COMMAND_USERS', []) or []

    async def cog_check(self, ctx: commands.Context):
        """
        Global check for all commands in this cog. Only users whose ID is in set in config may execute commands.
        """
        cmd_name = ctx.command.name if ctx.command else 'unknown'
        if ctx.user.id in self.allowed_users:
            logger.info(f"User {ctx.user.id} executed the '{cmd_name}' command.")
            return True
        # deny access otherwise
        await ctx.respond("You are not authorized to use this command.", ephemeral=True)
        logger.warning(f"Unauthorized access: user {ctx.user.id} attempted to run command '{cmd_name}'")
        return False
    
    @commands.Cog.listener()
    async def on_application_command_error(self, interaction, error):
        """
        Handle errors during slash‐command execution.

        Unwraps the original exception if it was wrapped in an ApplicationCommandInvokeError,
        silently ignores permission‐related CheckFailure errors to avoid log spam when
        unauthorized users invoke protected commands, and logs all other exceptions
        with full tracebacks for debugging.
        """
        orig = getattr(error, "original", None)
        err  = orig or error

        if isinstance(err, (commands.CheckFailure, discord_errors.CheckFailure)):
            return

        cmd = interaction.command.name if interaction.command else "<unknown>"
        logger.exception(f"Error in command '{cmd}':", exc_info=err)
    
    @commands.Cog.listener()
    async def on_ready(self):
        """
        Fired when the bot is ready. Logs allowed users status.
        """
        if not self.allowed_users:
            logger.warning("No allowed users configured: commands will not work for anyone.")
        else:
            logger.info(f"Commands enabled for users: {self.allowed_users}")

    @commands.slash_command(
        name="ping_server",
        description="Show server latency and server information.",
        guild_ids=[GUILD_ID],
    )
    async def ping(self, ctx: discord.ApplicationContext):
        """Responds with bot latency, server name, member count, and uptime."""
        latency_ms = self.bot.latency * 1000
        uptime_seconds = time.time() - self.start_time
        hours, remainder = divmod(int(uptime_seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        uptime_str = f"{hours}h {minutes}m {seconds}s"
        guild = ctx.guild

        embed = discord.Embed(title="📡 Pong! (Server)", timestamp=datetime.utcnow())
        embed.add_field(name="Latency", value=f"{latency_ms:.2f} ms", inline=True)
        embed.add_field(name="Server", value=guild.name, inline=True)
        embed.add_field(name="Members", value=str(guild.member_count), inline=True)
        embed.add_field(name="Uptime", value=uptime_str, inline=True)

        await ctx.respond(embed=embed, ephemeral=True)
        
    @commands.slash_command(
        name="ping_client",
        description="Show client latency and server information.",
        guild_ids=[GUILD_ID],
    )
    async def ping_client(self, ctx: discord.ApplicationContext):
        """Responds with gateway latency, round‑trip time, client uptime, and timestamps."""
        await ctx.defer(ephemeral=True)

        server_ts = datetime.now(timezone.utc).timestamp()
        resp = await self.bot.ws_manager.request({
            "type": "ping",
            "data": {"timestamp": server_ts}
        })

        if not resp or "data" not in resp:
            return await ctx.followup.send(
                "No response from client (timed out or error)",
                ephemeral=True
            )

        d = resp["data"]
        ws_latency_ms   = (d.get("discord_ws_latency_s") or 0) * 1000
        round_trip_ms   = (d.get("round_trip_seconds") or 0) * 1000
        client_start    = datetime.fromisoformat(d.get("client_start_time"))
        uptime_delta    = datetime.now(timezone.utc) - client_start
        hours, rem      = divmod(int(uptime_delta.total_seconds()), 3600)
        minutes, sec    = divmod(rem, 60)
        uptime_str      = f"{hours}h {minutes}m {sec}s"

        embed = discord.Embed(
            title="📡 Pong! (Client)",
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="Latency", value=f"{ws_latency_ms:.2f} ms", inline=True)
        embed.add_field(name="Round‑Trip Time", value=f"{round_trip_ms:.2f} ms", inline=True)
        embed.add_field(name="Client Uptime", value=uptime_str, inline=True)

        await ctx.followup.send(embed=embed, ephemeral=True)
        
    @commands.slash_command(
        name="block_add",
        description="Add or remove a keyword from the block list.",
        guild_ids=[GUILD_ID],
    )
    async def block_add(
        self,
        ctx: discord.ApplicationContext,
        keyword: str = Option(description="Keyword to block (will toggle)", required=True),
    ):
        """Toggle a blocked keyword in blocked_keywords."""
        if self.db.add_blocked_keyword(keyword):
            action, emoji = "added", "✅"
        elif self.db.remove_blocked_keyword(keyword):
            action, emoji = "removed", "🗑️"
        else:
            await ctx.respond(f"⚠️ Couldn’t toggle `{keyword}`.", ephemeral=True)
            return

        # push update to client
        new_list = self.db.get_blocked_keywords()
        await self.bot.ws_manager.send({
            "type": "settings_update",
            "data": {"blocked_keywords": new_list}
        })

        await ctx.respond(f"{emoji} `{keyword}` {action} in block list.", ephemeral=True)

    @commands.slash_command(
        name="block_list",
        description="List all blocked keywords.",
        guild_ids=[GUILD_ID],
    )
    async def block_list(self, ctx: discord.ApplicationContext):
        """Show currently blocked keywords."""
        kws = self.db.get_blocked_keywords()
        if not kws:
            await ctx.respond("📋 Your block list is empty.", ephemeral=True)
        else:
            formatted = "\n".join(f"• `{kw}`" for kw in kws)
            await ctx.respond(f"📋 **Blocked keywords:**\n{formatted}", ephemeral=True)

        
def setup(bot: commands.Bot):
    bot.add_cog(CloneCommands(bot))
