# modules/telethon_collector.py
"""
Telethon-based collector for public Telegram channels and groups.

Supports:
    - Public broadcast channels
    - Public megagroups and legacy groups

Per-chat extraction:
    1. Full chat metadata (title, description, subscribers, type, flags, etc.)
    2. Per-message metadata aligned with the TeleScope dataset schema
       (Gangopadhyay et al., ICWSM 2025), including:
           - message text, timestamp, author_id
           - reply_to_message_id     → reconstructs conversation threads
           - views, forwards count, reactions  → engagement metrics
           - edit_date               → detects post-publication edits
           - is_pinned               → pinned messages flag
           - forward provenance      → forwarded_from_id/type/date
           - Telegram entities       → bold, italic, URLs, hashtags, mentions
           - outbound links and @usernames extracted from text (snowball fuel)
    3. Snowball targets: usernames discovered in forwards and text mentions

NEVER writes to storage directly — returns structured dataclasses for the
orchestrator (main.py) to handle persistence decisions.
"""

from __future__ import annotations

import asyncio
import configparser
import datetime
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from telethon import TelegramClient, functions, types
from telethon.errors import (ChannelPrivateError, FloodWaitError,
                             UsernameInvalidError, UsernameNotOccupiedError)
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.functions.messages import GetFullChatRequest

from config.settings import CHAT_SLEEP_SEC, ITER_SLEEP_SEC

logger = logging.getLogger(__name__)

# Regex patterns for text entity extraction
_TG_MENTION_RE = re.compile(r"@([\w]{5,32})")
_TG_LINK_RE    = re.compile(r"t(?:elegram)?\.me/([\w]{5,32})", re.IGNORECASE)
_URL_RE        = re.compile(r"https?://[^\s]+")
_HASHTAG_RE    = re.compile(r"#(\w+)")


# Return dataclasses

@dataclass
class ChatMetadata:
    """
    Metadata for a single Telegram channel or group.

    Fields mirror the TeleScope enriched channel metadata schema
    (Section 4.1) with additions for groups (megagroup flag).
    """
    telegram_id:      int
    username:         Optional[str]
    title:            Optional[str]
    description:      Optional[str]       # 'about' field from GetFullChannelRequest
    chat_type:        str                 # "channel" | "group" | "unknown"
    n_subscribers:    Optional[int]       # participants_count
    is_scam:          bool
    is_fake:          bool                # impersonation flag (TeleScope Table 4)
    is_verified:      bool
    is_restricted:    bool                # e.g. adult / copyright restriction
    restriction_reason: Optional[str]     # reason string when is_restricted=True
    is_bot:           bool
    creation_date:    Optional[datetime.datetime]


@dataclass
class TelegramEntity:
    """
    A structural entity within a message (TeleScope Section 4.2).

    Telegram shares entity positions as spans; we extract the actual
    text value for immediate usability — same enrichment done in TeleScope.

    Entity types (non-exhaustive):
        bold, italic, underline, strikethrough, code, pre,
        url, text_url, mention, mention_name, hashtag, cashtag,
        bot_command, email, phone_number, bank_card
    """
    type:       str            # entity type string
    value:      str            # extracted text for the span
    offset:     int            # byte offset in message text
    length:     int            # byte length of the span
    url:        Optional[str]  # only populated for MessageEntityTextUrl


@dataclass
class ReactionCount:
    """Aggregated count for a single reaction emoji."""
    emoticon:  str 
    count:     int
    is_chosen: bool  # whether the collecting account reacted with this


@dataclass
class CollectedMessage:
    """
    Full metadata for a single Telegram message.

    Schema designed to support:
        - Thread reconstruction  (reply_to_message_id)
        - Multi-author mapping   (author_id)
        - Engagement analysis    (views, forwards_count, reactions)
        - Edit tracking          (edit_date)
        - Information propagation (is_forwarded, forwarded_from_*)
        - Entity-based search    (entities, hashtags, outbound_links)
        - Snowball discovery     (outbound_tg_usernames)

    Aligns with the TeleScope message metadata schema (Section 4.2) and
    the Telethon Message constructor fields.
    """
    # Identifiers
    _id:                    str            # "{chat_id}_{message_id}"  (MongoDB PK)
    chat_id:                int            # numeric telegram_id of parent chat
    chat_username:          Optional[str]
    message_id:             int            # unique within the channel

    # Content
    text:                   str
    date:                   datetime.datetime   # UTC timestamp of original post
    edit_date:              Optional[datetime.datetime]  # last edit timestamp
    is_pinned:              bool

    # Authorship
    author_id:              Optional[int]   # user_id of sender (groups) or None (channels)
    is_bot_author:          bool            # whether the sender is a bot
    is_verified_author:     bool            # whether the sender is verified

    # Threading
    reply_to_message_id:    Optional[int]   # message_id being replied to

    # Forwarding provenance
    is_forwarded:           bool
    forwarded_from_id:      Optional[int]   # numeric id of origin chat/user
    forwarded_from_type:    Optional[str]   # "channel" | "user" | "chat"
    forwarded_from_name:    Optional[str]   # display name of origin (if available)
    forwarded_date:         Optional[datetime.datetime]  # original post datetime
    forwarded_message_id:   Optional[int]   # message_id in the origin channel

    # Engagement metrics (TeleScope Section 4.2 + 4.3)
    views:                  Optional[int]   # view count (channels only)
    forwards_count:         Optional[int]   # number of times forwarded (per-channel)
    reactions:              list[ReactionCount] = field(default_factory=list)

    # Media
    has_media:              bool = False
    media_type:             Optional[str] = None   # photo/video/document/gif/...

    # Entities (TeleScope Section 4.2)
    entities:               list[TelegramEntity] = field(default_factory=list)
    hashtags:               list[str] = field(default_factory=list)
    outbound_links:         list[str] = field(default_factory=list)
    outbound_tg_usernames:  list[str] = field(default_factory=list)


@dataclass
class CollectionResult:
    """Return value of TelegramCollector.collect_messages()."""
    metadata:           ChatMetadata
    messages:           list[CollectedMessage] = field(default_factory=list)
    snowball_usernames: list[str]              = field(default_factory=list)


# Helper functions

def _peer_id_type(peer) -> tuple[Optional[int], Optional[str]]:
    """Resolves a Peer object to (numeric_id, type_string)."""
    if isinstance(peer, types.PeerChannel):
        return peer.channel_id, "channel"
    if isinstance(peer, types.PeerUser):
        return peer.user_id, "user"
    if isinstance(peer, types.PeerChat):
        return peer.chat_id, "chat"
    return None, None


def _extract_tg_targets(text: str) -> list[str]:
    """Extracts @usernames and t.me/ links from message text for snowball."""
    mentions = _TG_MENTION_RE.findall(text)
    links    = _TG_LINK_RE.findall(text)
    combined = [u.lower() for u in mentions + links]
    return list(dict.fromkeys(combined))


def _extract_hashtags(text: str) -> list[str]:
    """Extracts #hashtags from message text."""
    return [h.lower() for h in _HASHTAG_RE.findall(text)]


def _classify_media_type(message) -> Optional[str]:
    """
    Classifies the media attachment type of a message.
    Returns a human-readable string or None.
    """
    if not message.media:
        return None
    media = message.media
    if isinstance(media, types.MessageMediaPhoto):
        return "photo"
    if isinstance(media, types.MessageMediaDocument):
        if message.gif:    return "gif"
        if message.sticker: return "sticker"
        if message.video:  return "video"
        if message.audio:  return "audio"
        if message.voice:  return "voice_note"
        return "document"
    if isinstance(media, types.MessageMediaWebPage):
        return "webpage_preview"
    if isinstance(media, types.MessageMediaPoll):
        return "poll"
    if isinstance(media, types.MessageMediaGeo):
        return "geo_location"
    if isinstance(media, types.MessageMediaContact):
        return "contact"
    return "other"


def _extract_entities(message) -> list[TelegramEntity]:
    """
    Converts raw Telegram entity spans to TelegramEntity objects with
    extracted text values — same enrichment strategy as TeleScope Section 4.2.

    Telegram provides only the offset+length span; we resolve the actual
    text substring here for immediate usability.
    """
    if not message.entities or not message.message:
        return []

    text    = message.message
    result: list[TelegramEntity] = []

    for ent in message.entities:
        # Resolve text span (Telegram uses UTF-16 offsets in some cases,
        # but Telethon normalises them to Python string indices)
        try:
            value = text[ent.offset: ent.offset + ent.length]
        except (IndexError, TypeError):
            value = ""

        # Determine entity type string
        type_map = {
            types.MessageEntityBold:          "bold",
            types.MessageEntityItalic:        "italic",
            types.MessageEntityUnderline:     "underline",
            types.MessageEntityStrike:        "strikethrough",
            types.MessageEntityCode:          "code",
            types.MessageEntityPre:           "pre",
            types.MessageEntityUrl:           "url",
            types.MessageEntityTextUrl:       "text_url",
            types.MessageEntityMention:       "mention",
            types.MessageEntityMentionName:   "mention_name",
            types.MessageEntityHashtag:       "hashtag",
            types.MessageEntityCashtag:       "cashtag",
            types.MessageEntityBotCommand:    "bot_command",
            types.MessageEntityEmail:         "email",
            types.MessageEntityPhone:         "phone_number",
            types.MessageEntityBankCard:      "bank_card",
            types.MessageEntitySpoiler:       "spoiler",
            types.MessageEntityBlockquote:    "blockquote",
        }
        ent_type = type_map.get(type(ent), type(ent).__name__)
        url      = getattr(ent, "url", None)

        result.append(TelegramEntity(
            type   = ent_type,
            value  = value,
            offset = ent.offset,
            length = ent.length,
            url    = url,
        ))

    return result


def _extract_reactions(message) -> list[ReactionCount]:
    """
    Extracts per-emoji reaction counts from a message.
    Reactions are available for channels via the API (TeleScope Section 4.3).
    """
    if not hasattr(message, "reactions") or not message.reactions:
        return []

    reactions = message.reactions
    result: list[ReactionCount] = []

    # reactions.results is a list of ReactionCount objects
    for rc in getattr(reactions, "results", []):
        reaction = rc.reaction
        # Reactions can be emoji or custom emoji
        if isinstance(reaction, types.ReactionEmoji):
            emoticon = reaction.emoticon
        elif isinstance(reaction, types.ReactionCustomEmoji):
            emoticon = f"custom:{reaction.document_id}"
        else:
            emoticon = str(reaction)

        result.append(ReactionCount(
            emoticon  = emoticon,
            count     = rc.count,
            is_chosen = getattr(rc, "chosen", False) or False,
        ))

    return result


# Collector class

class TelegramCollector:
    """
    Async Telethon wrapper for collecting public channels and groups.

    A single instance is shared by the pipeline orchestrator (main.py)
    across all chat collections within a session.

    Args:
        config_file: Path to the INI configuration file.
    """

    def __init__(self, config_file: str = "config/config.ini"):
        cfg = configparser.ConfigParser()
        cfg.read(config_file)

        self.api_id   = cfg["Telegram"]["api_id"]
        self.api_hash = cfg["Telegram"]["api_hash"]
        self.phone    = cfg["Telegram"]["phone"]
        self.username = cfg["Telegram"]["username"]

        self.client = TelegramClient(
            f"sessions/{self.username}",
            self.api_id,
            self.api_hash,
        )

    # Authentication

    async def start(self) -> None:
        await self.client.start(phone=self.phone)
        logger.info("[telethon] Client started and authenticated.")

    async def stop(self) -> None:
        await self.client.disconnect()
        logger.info("[telethon] Client disconnected.")

    # Chat metadata

    async def _get_chat_metadata(self, entity) -> ChatMetadata:
        """
        Fetches full metadata for a channel or group entity.

        Calls GetFullChannelRequest / GetFullChatRequest to get the extended
        'about' field and accurate participant count — same approach as the
        TeleScope crawler (Section 3.2).
        """
        title             = getattr(entity, "title", None)
        username          = getattr(entity, "username", None)
        creation_dt       = getattr(entity, "date", None)
        is_scam           = bool(getattr(entity, "scam",        False))
        is_fake           = bool(getattr(entity, "fake",        False))
        is_verified       = bool(getattr(entity, "verified",    False))
        is_restricted     = bool(getattr(entity, "restricted",  False))
        is_bot            = bool(getattr(entity, "bot",         False))
        n_subs            = getattr(entity, "participants_count", None)
        description       = ""
        chat_type         = "unknown"
        restriction_reason: Optional[str] = None

        # Extract restriction reason if present
        if is_restricted and getattr(entity, "restriction_reason", None):
            reasons = entity.restriction_reason
            if reasons:
                restriction_reason = "; ".join(
                    getattr(r, "reason", str(r)) for r in reasons
                )

        if isinstance(entity, types.Channel):
            chat_type = "group" if entity.megagroup else "channel"
            try:
                full        = await self.client(GetFullChannelRequest(channel=entity))
                description = full.full_chat.about or ""
                n_subs      = full.full_chat.participants_count
                username    = full.chats[0].username
                is_scam     = bool(getattr(full.chats[0], "scam",     False))
                is_fake     = bool(getattr(full.chats[0], "fake",     False))
                is_verified = bool(getattr(entity,        "verified", False))
            except FloodWaitError as exc:
                logger.warning(f"[telethon] FloodWait on GetFullChannelRequest: {exc.seconds}s")
                await asyncio.sleep(exc.seconds)
            except Exception as exc:
                logger.warning(f"[telethon] Could not fetch full channel info: {exc}")

        elif isinstance(entity, types.Chat):
            chat_type = "group"
            try:
                full        = await self.client(GetFullChatRequest(chat_id=entity.id))
                description = full.full_chat.about or ""
                n_subs      = full.full_chat.participants_count
            except Exception as exc:
                logger.warning(f"[telethon] Could not fetch full chat info: {exc}")

        return ChatMetadata(
            telegram_id        = entity.id,
            username           = username,
            title              = title,
            description        = description,
            chat_type          = chat_type,
            n_subscribers      = n_subs,
            is_scam            = is_scam,
            is_fake            = is_fake,
            is_verified        = is_verified,
            is_restricted      = is_restricted,
            restriction_reason = restriction_reason,
            is_bot             = is_bot,
            creation_date      = creation_dt,
        )

    # Message collection

    async def collect_messages(
        self,
        username: str,
        limit:    int = 100,
    ) -> CollectionResult:
        """
        Collects up to `limit` messages from a public channel or group.

        The collected metadata follows the TeleScope enriched message schema
        (Section 4.2), extended with engagement metrics (views, forwards,
        reactions) used for aggregated user interaction analysis (Section 4.3).

        Args:
            username: Telegram username of the target chat (without @).
            limit:    Maximum number of messages to collect.

        Returns:
            CollectionResult containing chat metadata, collected messages,
            and a deduplicated list of snowball target usernames.

        Raises:
            ValueError:   If the username is invalid or not found.
            RuntimeError: On unexpected API errors.
        """
        logger.info(f"[telethon] Collecting '{username}' (limit={limit})")

        try:
            entity = await self.client.get_entity(username)
        except (UsernameInvalidError, UsernameNotOccupiedError) as exc:
            raise ValueError(f"Username not found or invalid: '{username}'") from exc
        except Exception as exc:
            raise RuntimeError(f"Failed to resolve entity '{username}': {exc}") from exc

        metadata      = await self._get_chat_metadata(entity)
        messages:       list[CollectedMessage] = []
        snowball_set:   set[str]               = set()

        async for msg in self.client.iter_messages(entity, limit=limit):
            if not isinstance(msg, types.Message):
                continue

            text = msg.message or ""

            # Skip completely empty messages (no text AND no media)
            if not text and not msg.media:
                continue

            # Forward provenance
            is_forwarded          = bool(msg.forward)
            fwd_id:   Optional[int] = None
            fwd_type: Optional[str] = None
            fwd_name: Optional[str] = None
            fwd_date: Optional[datetime.datetime] = None
            fwd_msg_id: Optional[int] = None

            if msg.forward:
                fwd_id, fwd_type = _peer_id_type(msg.forward.from_id)
                fwd_date         = msg.forward.date
                fwd_name         = getattr(msg.forward, "from_name", None)
                fwd_msg_id       = getattr(msg.forward, "channel_post", None)

                # Snowball: resolve username of the forwarding source channel
                if (
                    msg.forward.channel_post
                    and isinstance(msg.forward.from_id, types.PeerChannel)
                ):
                    try:
                        fwd_entity = await self.client.get_entity(msg.forward.from_id)
                        if getattr(fwd_entity, "username", None):
                            snowball_set.add(fwd_entity.username.lower())
                    except Exception:
                        pass  # silently skip unresolvable forward sources

            # Authorship
            author_id:         Optional[int] = None
            is_bot_author:     bool          = False
            is_verified_author: bool         = False

            if msg.from_id:
                author_id, _ = _peer_id_type(msg.from_id)

            # sender_id resolved to a User object when available
            sender = getattr(msg, "sender", None)
            if sender:
                is_bot_author      = bool(getattr(sender, "bot",      False))
                is_verified_author = bool(getattr(sender, "verified", False))

            # Threading
            reply_to_id: Optional[int] = None
            if msg.reply_to and hasattr(msg.reply_to, "reply_to_msg_id"):
                reply_to_id = msg.reply_to.reply_to_msg_id

            # Engagement metrics (TeleScope Sections 4.2 + 4.3)
            views          = getattr(msg, "views",    None)
            forwards_count = getattr(msg, "forwards", None)
            reactions      = _extract_reactions(msg)

            # Entities (TeleScope Section 4.2)
            tg_entities = _extract_entities(msg)
            hashtags    = _extract_hashtags(text) if text else []
            tg_targets  = _extract_tg_targets(text) if text else []
            urls        = _URL_RE.findall(text) if text else []

            # Mentions in text go into the snowball queue
            snowball_set.update(tg_targets)

            # Media
            media_type = _classify_media_type(msg)

            collected_msg = CollectedMessage(
                _id                   = f"{metadata.telegram_id}_{msg.id}",
                chat_id               = metadata.telegram_id,
                chat_username         = metadata.username,
                message_id            = msg.id,
                text                  = text,
                date                  = msg.date,
                edit_date             = getattr(msg, "edit_date", None),
                is_pinned             = bool(getattr(msg, "pinned", False)),
                author_id             = author_id,
                is_bot_author         = is_bot_author,
                is_verified_author    = is_verified_author,
                reply_to_message_id   = reply_to_id,
                is_forwarded          = is_forwarded,
                forwarded_from_id     = fwd_id,
                forwarded_from_type   = fwd_type,
                forwarded_from_name   = fwd_name,
                forwarded_date        = fwd_date,
                forwarded_message_id  = fwd_msg_id,
                views                 = views,
                forwards_count        = forwards_count,
                reactions             = reactions,
                has_media             = bool(msg.media),
                media_type            = media_type,
                entities              = tg_entities,
                hashtags              = hashtags,
                outbound_links        = urls,
                outbound_tg_usernames = tg_targets,
            )
            messages.append(collected_msg)
            await asyncio.sleep(ITER_SLEEP_SEC)

        # Remove the chat's own username from snowball targets
        if metadata.username:
            snowball_set.discard(metadata.username.lower())

        logger.info(
            f"[telethon] '{username}' → {len(messages)} messages, "
            f"{len(snowball_set)} snowball targets"
        )
        await asyncio.sleep(CHAT_SLEEP_SEC)

        return CollectionResult(
            metadata           = metadata,
            messages           = messages,
            snowball_usernames = list(snowball_set),
        )

    # Context manager

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *args):
        await self.stop()
