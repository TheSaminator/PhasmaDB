import random
import re
from typing import Optional, Any

import aiohttp
from aiohttp import web
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.serialization import load_pem_public_key

USERNAME_REGEX = re.compile("[0-9A-Za-z_]+")


async def read_json(ws: web.WebSocketResponse) -> Optional[Any]:
	msg = await ws.receive()
	if msg.type == aiohttp.WSMsgType.TEXT:
		return msg.json()
	elif msg.type == aiohttp.WSMsgType.CLOSE:
		print(f'ws connection closed normally')
	elif msg.type == aiohttp.WSMsgType.ERROR:
		print(f'ws connection closed with exception {ws.exception()}')
	else:
		print(f'unknown message type {msg.type}')
	
	return None


async def load_public_key(username: str) -> Optional[rsa.RSAPublicKey]:
	try:
		with open(f"public_keys/{username}.pem", "rb") as public_key_file:
			return load_pem_public_key(public_key_file.read())
	except OSError:
		pass
	except ValueError:
		pass
	return None


async def handshake(ws: web.WebSocketResponse) -> Optional[str]:
	# Load user data
	login_msg = await read_json(ws)
	username = login_msg['username']
	if not USERNAME_REGEX.fullmatch(username):
		await ws.send_json({'challenge': None, 'error': 101})
		return None
	public_key = await load_public_key(username)
	if not public_key:
		await ws.send_json({'challenge': None, 'error': 101})
		return None
	
	# Authenticate
	token = random.SystemRandom().randbytes(64)
	encrypted_token = public_key.encrypt(token, padding.PKCS1v15())
	await ws.send_json({'challenge': encrypted_token.hex()})
	
	# Check
	response = await read_json(ws)
	if not response:
		await ws.send_json({'success': False, 'error': 2})
		return None
	
	response = bytes.fromhex(response['response'])
	if response != token:
		await ws.send_json({'success': False, 'error': 102})
		return None
	
	return username
