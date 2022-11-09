import json
from typing import Iterable, Optional

import aiohttp
from aiohttp import web
from bson import ObjectId
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase


async def setup_db() -> AsyncIOMotorDatabase:
	db = AsyncIOMotorClient().phasmadb
	return db

routes = web.RouteTableDef()


@routes.get("/")
async def index(request: web.Request) -> web.WebSocketResponse:
	ws = web.WebSocketResponse()
	await ws.prepare(request)
	# TODO Do handshake here
	while True:
		msg = await ws.receive()
		if msg.type == aiohttp.WSMsgType.TEXT:
			pass
		elif msg.type == aiohttp.WSMsgType.ERROR:
			print(f'ws connection closed with exception {ws.exception()}')
			break
	
	return ws


async def create_app():
	global routes
	app_db = await setup_db()
	
	app = web.Application()
	
	app["db"] = app_db
	app.add_routes(routes)
	return app


if __name__ == '__main__':
	web.run_app(create_app())
