from typing import Optional

from aiohttp import web
from collections import namedtuple


UserInfo = namedtuple("UserInfo", ["username", "public_key"])


async def handshake(ws: web.WebSocketResponse) -> Optional[UserInfo]:
	pass
