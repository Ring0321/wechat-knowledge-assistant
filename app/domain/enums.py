from enum import StrEnum


class SourceType(StrEnum):
    NOTE = "note"
    IMAGE = "image"
    PDF = "pdf"
    WORD = "word"
    EXCEL = "excel"
    PPT = "ppt"
    AUDIO = "audio"
    VIDEO = "video"
    WEB_PAGE = "web_page"
    WECHAT_ARTICLE = "wechat_article"
    WECHAT_CHANNEL = "wechat_channel"
    OTHER = "other"


class SourceStatus(StrEnum):
    RECEIVED = "received"
    PROCESSING = "processing"
    STORED = "stored"
    READY = "ready"
    METADATA_ONLY = "metadata_only"
    FAILED = "failed"
    DELETING = "deleting"
    DELETED = "deleted"


class JobStatus(StrEnum):
    QUEUED = "queued"
    DOWNLOADING = "downloading"
    PARSING = "parsing"
    TRANSCRIBING = "transcribing"
    INDEXING = "indexing"
    COMPLETED = "completed"
    FAILED = "failed"


class MessageRole(StrEnum):
    USER = "user"
    ASSISTANT = "assistant"
    SYSTEM = "system"


class AssetKind(StrEnum):
    ORIGINAL = "original"
    NORMALIZED = "normalized"
    AUDIO = "audio"
    KEYFRAME = "keyframe"
    TRANSCRIPT = "transcript"
