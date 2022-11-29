import asyncio
import base64
import hashlib
import json
import random
from typing import NamedTuple, Optional, Any, Literal, Dict
from asyncio import Queue

import aiohttp
import phe as paillier
from cryptography.hazmat.primitives.asymmetric import rsa, padding
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption, load_pem_private_key, load_pem_public_key
from pyope.ope import OPE, ValueRange


class PhasmaDBLoginCredential(NamedTuple):
	username: str
	private_key: rsa.RSAPrivateKey
	
	@classmethod
	def load(cls, from_file: str) -> "PhasmaDBLoginCredential":
		with open(from_file) as file:
			credential = json.load(file)
			username = credential['username']
			private_key = load_pem_private_key(base64.b64decode(credential['private_key']), None)
			return PhasmaDBLoginCredential(username, private_key)


class PhasmaDBLocalKeyring(NamedTuple):
	rsa_public_key: rsa.RSAPublicKey
	rsa_private_key: rsa.RSAPrivateKey
	paillier_public_key: paillier.PaillierPublicKey
	paillier_private_key: paillier.PaillierPrivateKey
	aes_key: AES
	ope_key: OPE
	name_salt: bytes
	
	@classmethod
	def create(cls) -> "PhasmaDBLocalKeyring":
		rsa_sk = rsa.generate_private_key(
			public_exponent=65537,
			key_size=4096
		)
		rsa_pk = rsa_sk.public_key()
		
		(paillier_pk, paillier_sk) = paillier.generate_paillier_keypair()
		
		sys_rand = random.SystemRandom()
		aes_k = AES(sys_rand.randbytes(32))
		ope_k = OPE(sys_rand.randbytes(32))
		salt = sys_rand.randbytes(32)
		return PhasmaDBLocalKeyring(rsa_pk, rsa_sk, paillier_pk, paillier_sk, aes_k, ope_k, salt)
	
	@classmethod
	def load(cls, from_json: str) -> "PhasmaDBLocalKeyring":
		keyring = json.loads(from_json)
		rsa_pk = load_pem_public_key(base64.b64decode(keyring['rsa_pk']))
		rsa_sk = load_pem_private_key(base64.b64decode(keyring['rsa_sk']), None)
		paillier_pk = paillier.PaillierPublicKey(keyring['paillier_pk'])
		paillier_sk = paillier.PaillierPrivateKey(paillier_pk, keyring['paillier_sk']['p'], keyring['paillier_sk']['q'])
		aes_k = AES(base64.b64decode(keyring['aes_k']))
		ope_k = OPE(base64.b64decode(keyring['ope_k']['k']), ValueRange(keyring['ope_k']['plain_min'], keyring['ope_k']['plain_max']), ValueRange(keyring['ope_k']['cipher_min'], keyring['ope_k']['cipher_max']))
		salt = base64.b64decode(keyring['salt'])
		return PhasmaDBLocalKeyring(rsa_pk, rsa_sk, paillier_pk, paillier_sk, aes_k, ope_k, salt)
	
	def save(self) -> str:
		rsa_pk = base64.b64encode(self.rsa_public_key.public_bytes(encoding=Encoding.PEM, format=PublicFormat.PKCS1)).decode('ascii')
		rsa_sk = base64.b64encode(self.rsa_private_key.private_bytes(encoding=Encoding.PEM, format=PrivateFormat.PKCS8, encryption_algorithm=NoEncryption())).decode('ascii')
		paillier_pk = self.paillier_public_key.n
		paillier_sk = {'p': self.paillier_private_key.p, 'q': self.paillier_private_key.q}
		aes_k = base64.b64encode(self.aes_key.key).decode('ascii')
		ope_k = {'k': base64.b64encode(self.ope_key.key).decode('ascii'), 'plain_min': self.ope_key.in_range.start, 'plain_max': self.ope_key.in_range.end, 'cipher_min': self.ope_key.out_range.start, 'cipher_max': self.ope_key.out_range.end}
		salt = base64.b64encode(self.name_salt).decode('ascii')
		keyring = {
			'rsa_pk': rsa_pk,
			'rsa_sk': rsa_sk,
			'paillier_pk': paillier_pk,
			'paillier_sk': paillier_sk,
			'aes_k': aes_k,
			'ope_k': ope_k,
			'salt': salt
		}
		return json.dumps(keyring)


async def read_json(ws: aiohttp.ClientWebSocketResponse) -> Optional[Any]:
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


IndexType = Literal['add', 'multiply', 'sort', 'unique']

ERROR_TYPES = {
	1: 'Command type does not exist',
	2: 'Request is improperly formatted',
	101: 'User does not exist',
	102: 'Authentication bytes did not match',
	201: 'Table does not exist',
	202: 'Table already exists',
	301: 'Row with same ID already exists',
	302: 'Row with same unique value already exists',
	303: 'Not all indexed columns have values'
}


class PhasmaDBError(Exception):
	def __init__(self, code: int):
		self._code = code
		self._message = ERROR_TYPES[code]
	
	@property
	def code(self) -> int:
		return self._code
	
	@property
	def message(self) -> str:
		return self._message
	
	def __str__(self) -> str:
		return self._message
	
	def __repr__(self) -> str:
		return f'PhasmaDBError(code={self._code}, message={self._message})'


def hash_name(keyring: PhasmaDBLocalKeyring, name: str) -> str:
	hashed_name = hashlib.sha3_256()
	hashed_name.update(name.encode('utf-8'))
	hashed_name.update(keyring.name_salt)
	return hashed_name.digest().hex()


class PhasmaDBConnection:
	def __init__(self, server_url: str, credential: PhasmaDBLoginCredential, session: aiohttp.ClientSession):
		self._server_url = server_url
		self._credential = credential
		self._session = session
		self._cmd_id_counter = 0
		self._commands = Queue(4)
	
	async def loop(self) -> None:
		"""
		Should be run in parallel with the code that uses the database, using asyncio.create_task
		"""
		
		async with self._session.ws_connect(self._server_url) as ws:
			await ws.send_json({'username': self._credential.username})
			
			challenge = await read_json(ws)
			if not challenge['challenge']:
				raise PhasmaDBError(challenge['error'])
			token = self._credential.private_key.decrypt(bytes.fromhex(challenge['challenge']), padding.PKCS1v15())
			await ws.send_json({'response': token.hex()})
			
			while not ws.closed:
				(command, future) = await self._commands.get()
				
				cmd_id = self._cmd_id_counter
				self._cmd_id_counter += 1
				command['cmd_id'] = cmd_id
				
				await ws.send_json(command)
				response = await read_json(ws)
				if not response:
					break
				
				assert response['cmd_id'] == cmd_id
				
				future.set_result(response)
				self._commands.task_done()
	
	async def __send_command(self, command: Any) -> Any:
		future = asyncio.Future()
		await self._commands.put((command, future))
		return await future
	
	async def create_table(self, keyring: PhasmaDBLocalKeyring, name: str, indices: Dict[str, IndexType]):
		response = await self.__send_command({
			'cmd': 'create_table',
			'name': hash_name(keyring, name),
			'indices': indices
		})
		if not response['success']:
			raise PhasmaDBError(response['error'])
	
	async def close(self):
		await self.__send_command({'cmd': 'exit'})
