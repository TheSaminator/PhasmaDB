import base64
import hashlib
import json
import os
import random
import re
from asyncio import Future, Queue
from typing import NamedTuple, Optional, Any, Literal, Dict, Callable, List

import aiohttp
from cryptography.hazmat.primitives import padding as aes_padding
from cryptography.hazmat.primitives.asymmetric import rsa, padding as rsa_padding
from cryptography.hazmat.primitives.ciphers import Cipher
from cryptography.hazmat.primitives.ciphers.algorithms import AES
from cryptography.hazmat.primitives.ciphers.modes import CBC
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from pyope.ope import OPE, ValueRange


class PhasmaDBLoginCredential(NamedTuple):
	username: str
	private_key: rsa.RSAPrivateKey
	
	@classmethod
	def load(cls, from_file: str) -> "PhasmaDBLoginCredential":
		with open(from_file) as file:
			credential = json.load(file)
			username = credential['username']
			private_key = load_pem_private_key(credential['private_key'].encode('ascii'), None)
			return PhasmaDBLoginCredential(username, private_key)


class PhasmaDBLocalKeyring(NamedTuple):
	aes_key: AES
	ope_key: OPE
	name_salt: bytes
	
	@classmethod
	def create(cls) -> "PhasmaDBLocalKeyring":
		sys_rand = random.SystemRandom()
		aes_k = AES(sys_rand.randbytes(32))
		ope_k = OPE(sys_rand.randbytes(32), ValueRange(0, 2 ** 31 - 1), ValueRange(0, 2 ** 63 - 1))
		salt = sys_rand.randbytes(32)
		return PhasmaDBLocalKeyring(aes_k, ope_k, salt)
	
	@classmethod
	def load(cls, from_json: str) -> "PhasmaDBLocalKeyring":
		keyring = json.loads(from_json)
		aes_k = AES(base64.b64decode(keyring['aes_k']))
		ope_k = OPE(base64.b64decode(keyring['ope_k']['k']), ValueRange(keyring['ope_k']['plain_min'], keyring['ope_k']['plain_max']), ValueRange(keyring['ope_k']['cipher_min'], keyring['ope_k']['cipher_max']))
		salt = base64.b64decode(keyring['salt'])
		return PhasmaDBLocalKeyring(aes_k, ope_k, salt)
	
	def save(self) -> str:
		aes_k = base64.b64encode(self.aes_key.key).decode('ascii')
		ope_k = {'k': base64.b64encode(self.ope_key.key).decode('ascii'), 'plain_min': self.ope_key.in_range.start, 'plain_max': self.ope_key.in_range.end, 'cipher_min': self.ope_key.out_range.start, 'cipher_max': self.ope_key.out_range.end}
		salt = base64.b64encode(self.name_salt).decode('ascii')
		keyring = {
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


IndexType = Literal['sort', 'unique', 'text', 'unique_text']

ERROR_TYPES = {
	1: 'Command type does not exist',
	2: 'Request is improperly formatted',
	101: 'User does not exist',
	102: 'Authentication bytes did not match',
	201: 'Table does not exist',
	202: 'Table already exists',
	301: 'Row with that ID does not exist',
	302: 'Row with same unique value already exists',
	303: 'Not all indexed columns have values',
	304: 'Values specified for non-existent indices',
	305: 'Values incompatible with indices\' types'
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


class PhasmaDBTextData(NamedTuple):
	text: str
	index_type: Literal['plain', 'prefix', 'word']


class PhasmaDBDataRow(NamedTuple):
	indexed_data: Dict[str, int | str | PhasmaDBTextData]
	extra_data: Any


WORD_REGEX = re.compile("[0-9a-zA-Z]+")


def process_sent_data_cell(keyring: PhasmaDBLocalKeyring, cell: Any) -> Any:
	if isinstance(cell, int):
		return keyring.ope_key.encrypt(cell)
	elif isinstance(cell, str):
		# assume plain text-index
		return hash_name(keyring, cell)
	elif isinstance(cell, PhasmaDBTextData):
		if cell.index_type == 'plain':
			return hash_name(keyring, cell.text)
		elif cell.index_type == 'prefix':
			return [hash_name(keyring, cell.text[0:i]) for i in range(1, len(cell.text) + 1)]
		elif cell.index_type == 'word':
			return [hash_name(keyring, word) for word in WORD_REGEX.findall(cell.text)]
	return hash_name(keyring, str(cell))


def process_sent_data(keyring: PhasmaDBLocalKeyring, row: PhasmaDBDataRow) -> Dict:
	padder = aes_padding.ANSIX923(128).padder()
	data_to_encrypt = padder.update(json.dumps(row.extra_data).encode('utf-8')) + padder.finalize()
	iv = os.urandom(16)
	cipher = Cipher(keyring.aes_key, CBC(iv)).encryptor()
	encrypted_data = iv + cipher.update(data_to_encrypt) + cipher.finalize()
	
	return {
		'indexed': {hash_name(keyring, k): process_sent_data_cell(keyring, v) for (k, v) in row.indexed_data.items()},
		'extra': base64.b64encode(encrypted_data).decode('ascii')
	}


def get_column_hashes(keyring: PhasmaDBLocalKeyring, columns: List[str]) -> Dict[str, str]:
	return {hash_name(keyring, c): c for c in columns}


def process_received_data(keyring: PhasmaDBLocalKeyring, column_hashes: Dict[str, str], row: Dict) -> PhasmaDBDataRow:
	data_to_decrypt = base64.b64decode(row['extra'].encode('ascii'))
	iv = data_to_decrypt[0:16]
	data_to_decrypt = data_to_decrypt[16:]
	
	cipher = Cipher(keyring.aes_key, CBC(iv)).decryptor()
	decrypted_data = cipher.update(data_to_decrypt) + cipher.finalize()
	
	unpadder = aes_padding.ANSIX923(128).unpadder()
	decrypted_data = unpadder.update(decrypted_data) + unpadder.finalize()
	
	return PhasmaDBDataRow(
		indexed_data={column_hashes[k]: keyring.ope_key.decrypt(v) for (k, v) in row['indexed'].items() if k in column_hashes.keys() and isinstance(v, int)},
		extra_data=json.loads(decrypted_data.decode('utf-8'))
	)


SelectAll = None

SelectNodeType = Literal['and', 'or', 'not_and', 'not_or']
SelectLeafType = Literal['eq', 'neq', 'gt', 'lt', 'gte', 'lte', 'text']


class PhasmaDBQuerySelectNode(NamedTuple):
	node_type: SelectNodeType
	sub_nodes: List["PhasmaDBQuerySelectClause"]
	
	def __and__(self, other: "PhasmaDBQuerySelectClause") -> "PhasmaDBQuerySelectNode":
		if self.node_type == 'and':
			return PhasmaDBQuerySelectNode('and', [*self.sub_nodes, other])
		else:
			return PhasmaDBQuerySelectNode('and', [self, other])
	
	def __or__(self, other: "PhasmaDBQuerySelectClause") -> "PhasmaDBQuerySelectNode":
		if self.node_type == 'or':
			return PhasmaDBQuerySelectNode('or', [*self.sub_nodes, other])
		else:
			return PhasmaDBQuerySelectNode('or', [self, other])
	
	def __invert__(self) -> "PhasmaDBQuerySelectNode":
		if self.node_type.startswith('not_'):
			return PhasmaDBQuerySelectNode(self.node_type[4:], self.sub_nodes)
		else:
			return PhasmaDBQuerySelectNode('not_' + self.node_type, self.sub_nodes)


class PhasmaDBQuerySelectLeaf(NamedTuple):
	column_name: str
	leaf_type: SelectLeafType
	test_value: int | str | List[str]
	
	def __and__(self, other: "PhasmaDBQuerySelectLeaf") -> PhasmaDBQuerySelectNode:
		return PhasmaDBQuerySelectNode('and', [self, other])
	
	def __or__(self, other: "PhasmaDBQuerySelectLeaf") -> PhasmaDBQuerySelectNode:
		return PhasmaDBQuerySelectNode('or', [self, other])
	
	def __invert__(self) -> "PhasmaDBQuerySelectNode":
		return PhasmaDBQuerySelectNode('not_and', [self])


PhasmaDBQuerySelectClause = PhasmaDBQuerySelectNode | PhasmaDBQuerySelectLeaf


class Column(NamedTuple):
	plain_name: str
	
	def __eq__(self, other: int | str | List[str]) -> PhasmaDBQuerySelectLeaf:
		if isinstance(other, int):
			return PhasmaDBQuerySelectLeaf(self.plain_name, 'eq', other)
		else:
			return PhasmaDBQuerySelectLeaf(self.plain_name, 'text', other)
	
	def __ne__(self, other: int) -> PhasmaDBQuerySelectLeaf:
		return PhasmaDBQuerySelectLeaf(self.plain_name, 'neq', other)
	
	def __lt__(self, other: int) -> PhasmaDBQuerySelectLeaf:
		return PhasmaDBQuerySelectLeaf(self.plain_name, 'lt', other)
	
	def __gt__(self, other: int) -> PhasmaDBQuerySelectLeaf:
		return PhasmaDBQuerySelectLeaf(self.plain_name, 'gt', other)
	
	def __lte__(self, other: int) -> PhasmaDBQuerySelectLeaf:
		return PhasmaDBQuerySelectLeaf(self.plain_name, 'lte', other)
	
	def __gte__(self, other: int) -> PhasmaDBQuerySelectLeaf:
		return PhasmaDBQuerySelectLeaf(self.plain_name, 'gte', other)


SortOrder = Literal['asc', 'desc']


class PhasmaDBDataQuery(NamedTuple):
	select: PhasmaDBQuerySelectClause | SelectAll
	sort: List[tuple[str, SortOrder]] = []
	limit: Optional[int] = None


def process_sent_query_select_clause(keyring: PhasmaDBLocalKeyring, clause: PhasmaDBQuerySelectClause) -> Dict:
	if isinstance(clause, PhasmaDBQuerySelectNode):
		return {clause.node_type: [process_sent_query_select_clause(keyring, node) for node in clause.sub_nodes]}
	elif isinstance(clause, PhasmaDBQuerySelectLeaf):
		value = clause.test_value
		if isinstance(value, int):
			return {hash_name(keyring, clause.column_name): {clause.leaf_type: keyring.ope_key.encrypt(value)}}
		else:
			return {hash_name(keyring, clause.column_name): {clause.leaf_type: hash_name(keyring, value)}}
	return dict()


def process_sent_query(keyring: PhasmaDBLocalKeyring, query: PhasmaDBDataQuery) -> Dict:
	return {
		'filter': process_sent_query_select_clause(keyring, query.select),
		'sort': [(hash_name(keyring, k), v) for (k, v) in query.sort],
		'limit': query.limit
	}


def process_sent_deletion(keyring: PhasmaDBLocalKeyring, query: PhasmaDBQuerySelectClause | SelectAll) -> Dict:
	return process_sent_query_select_clause(keyring, query)


class PhasmaDBConnection:
	def __init__(self):
		self._commands = Queue()
		self._exception = None
	
	async def connection(self, server_url: str, credential: PhasmaDBLoginCredential, session: aiohttp.ClientSession) -> None:
		"""
		Should be run in parallel with the code that uses the database, using asyncio.create_task
		"""
		
		current_future = None
		try:
			async with session.ws_connect(server_url) as ws:
				await ws.send_json({'username': credential.username})
				
				challenge = await read_json(ws)
				if not challenge['challenge']:
					raise PhasmaDBError(challenge['error'])
				token = credential.private_key.decrypt(bytes.fromhex(challenge['challenge']), rsa_padding.PKCS1v15())
				await ws.send_json({'response': token.hex()})
				
				sent_exit = False
				
				while (not ws.closed) and (not sent_exit):
					(command, future) = await self._commands.get()
					current_future = future
					
					if command['cmd'] == 'exit':
						sent_exit = True
					
					await ws.send_json(command)
					response = await read_json(ws)
					if not response:
						break
					
					future.set_result(response)
					current_future = None
					
					self._commands.task_done()
		except aiohttp.client.ClientConnectionError as ex:
			self._exception = ex
			if current_future is not None:
				current_future.set_exception(ex)
			while not self._commands.empty():
				(_, future) = await self._commands.get()
				future.set_exception(ex)
	
	async def __send_command(self, command: Any) -> Any:
		if self._exception is not None:
			raise self._exception
		
		future = Future()
		await self._commands.put((command, future))
		return await future
	
	async def create_table(self, keyring: PhasmaDBLocalKeyring, name: str, indices: Dict[str, IndexType], on_error: Callable[[str, PhasmaDBError, Any], None]) -> bool:
		response = await self.__send_command({
			'cmd': 'create_table',
			'name': hash_name(keyring, name),
			'indices': {hash_name(keyring, k): v for (k, v) in indices.items()}
		})
		if not response['success']:
			on_error("create_table", PhasmaDBError(response['error']), (name,))
			return False
		return True
	
	async def insert_data(self, keyring: PhasmaDBLocalKeyring, table_name: str, data: Dict[str, PhasmaDBDataRow], on_error: Callable[[str, PhasmaDBError, Any], None]) -> Dict[str, bool]:
		response = await self.__send_command({
			'cmd': 'insert_data',
			'table': hash_name(keyring, table_name),
			'data': {k: process_sent_data(keyring, v) for (k, v) in data.items()}
		})
		results = {k: True for k in response['results'].keys()}
		for (row_id, result) in response['results'].items():
			if not result['success']:
				on_error("insert_data", PhasmaDBError(result['error']), (table_name, row_id))
				results[row_id] = False
		return results
	
	async def query_by_id(self, keyring: PhasmaDBLocalKeyring, table_name: str, row_id: str, requested_indices: List[str], on_error: Callable[[str, PhasmaDBError, Any], None]) -> Optional[PhasmaDBDataRow]:
		response = await self.__send_command({
			'cmd': 'query_by_id',
			'table': hash_name(keyring, table_name),
			'row_id': row_id
		})
		if not response['success']:
			on_error("query_by_id", PhasmaDBError(response['error']), (table_name, row_id))
		else:
			data = response['row']
			column_hashes = get_column_hashes(keyring, requested_indices)
			return process_received_data(keyring, column_hashes, data)
		return None
	
	async def query_data(self, keyring: PhasmaDBLocalKeyring, table_name: str, query: PhasmaDBDataQuery, requested_indices: List[str], on_error: Callable[[str, PhasmaDBError, Any], None]) -> Optional[Dict[str, PhasmaDBDataRow]]:
		response = await self.__send_command({
			'cmd': 'query_data',
			'table': hash_name(keyring, table_name),
			'query': process_sent_query(keyring, query)
		})
		if not response['success']:
			on_error("query_data", PhasmaDBError(response['error']), (table_name,))
		else:
			data = response['data']
			column_hashes = get_column_hashes(keyring, requested_indices)
			return {k: process_received_data(keyring, column_hashes, v) for (k, v) in data.items()}
		return None
	
	async def delete_by_id(self, keyring: PhasmaDBLocalKeyring, table_name: str, row_id: str, on_error: Callable[[str, PhasmaDBError, Any], None]) -> bool:
		response = await self.__send_command({
			'cmd': 'delete_by_id',
			'table': hash_name(keyring, table_name),
			'row_id': row_id
		})
		if not response['success']:
			on_error("delete_by_id", PhasmaDBError(response['error']), (table_name, row_id))
			return False
		return True
	
	async def delete_data(self, keyring: PhasmaDBLocalKeyring, table_name: str, query: PhasmaDBQuerySelectClause | SelectAll, on_error: Callable[[str, PhasmaDBError, Any], None]) -> Optional[int]:
		response = await self.__send_command({
			'cmd': 'delete_data',
			'table': hash_name(keyring, table_name),
			'filter': process_sent_deletion(keyring, query)
		})
		if not response['success']:
			on_error("delete_data", PhasmaDBError(response['error']), (table_name,))
		else:
			deleted_count = response['count']
			return deleted_count
		return None
	
	async def drop_table(self, keyring: PhasmaDBLocalKeyring, table_name: str, on_error: Callable[[str, PhasmaDBError, Any], None]) -> bool:
		response = await self.__send_command({
			'cmd': 'drop_table',
			'table': hash_name(keyring, table_name)
		})
		if not response['success']:
			on_error("drop_table", PhasmaDBError(response['error']), (table_name,))
			return False
		return True
	
	async def close(self):
		await self.__send_command({'cmd': 'exit'})
