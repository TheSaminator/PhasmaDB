import pymongo
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase

import dbfuncs
from auth import handshake, read_json


async def setup_db() -> AsyncIOMotorDatabase:
	db = AsyncIOMotorClient().phasmadb
	await db.tables.create_index([('name', pymongo.ASCENDING), ('owner', pymongo.ASCENDING)], unique=True)
	return db


routes = web.RouteTableDef()


@routes.get("/phasma-db")
async def index(request: web.Request) -> web.WebSocketResponse:
	ws = web.WebSocketResponse()
	await ws.prepare(request)
	
	username = await handshake(ws)
	if not username:
		await ws.close()
		return ws
	
	while True:
		req = await read_json(ws)
		if not req:
			break
		if req['cmd'] == 'exit':
			await ws.send_json({"farewell": True, 'cmd_id': req['cmd_id']})
			await ws.close()
			break
		res = await dbfuncs.process_command(request.app['db'], username, req)
		await ws.send_json(res)
	
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
