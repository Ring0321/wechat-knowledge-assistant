"""One fixed official reply ID for each message/purpose; no direct network I/O."""

import hashlib
import json
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.connectors.wecom.persistence import WeComOutbox


async def enqueue_reply(
    session: AsyncSession,
    *,
    corp_id: str,
    user_id: UUID,
    message_id: UUID,
    open_kfid: str,
    external_userid: str,
    received_at: datetime,
    content: str,
    purpose: str = "ack",
) -> None:
    reply_id = hashlib.sha256(
        json.dumps([corp_id, open_kfid, str(message_id), purpose]).encode()
    ).hexdigest()[:32]
    await session.execute(
        insert(WeComOutbox)
        .values(
            id=uuid4(),
            corp_id=corp_id,
            user_id=user_id,
            inbound_message_id=message_id,
            open_kfid=open_kfid,
            external_userid=external_userid,
            received_at=received_at,
            content=content,
            purpose=purpose,
            reply_msgid=reply_id,
        )
        .on_conflict_do_nothing(
            index_elements=[
                WeComOutbox.user_id,
                WeComOutbox.inbound_message_id,
                WeComOutbox.purpose,
            ]
        )
    )
