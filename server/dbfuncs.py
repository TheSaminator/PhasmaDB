import asyncio
import re
from enum import IntEnum
from typing import Any, NamedTuple, Generic, TypeVar, Optional, Union, Literal, Dict

import nanoid
from aiohttp import web
from motor.motor_asyncio import AsyncIOMotorDatabase


# Type hint support for results
class PhasmaDBErrorCode(IntEnum):
	COMMAND_TYPE_DOES_NOT_EXIST = 1
	REQUEST_IMPROPERLY_FORMATTED = 2
	USER_DOES_NOT_EXIST = 101
	AUTH_BYTES_NO_MATCH = 102
	TABLE_DOES_NOT_EXIST = 201
	TABLE_ALREADY_EXISTS = 202
	ROW_SAME_ID_ALREADY_EXISTS = 301
	ROW_SAME_UNIQUES_ALREADY_EXISTS = 302
	ROW_LACKS_SOME_INDEXED_VALUES = 303
	ROW_HAS_EXTRA_INDEXED_VALUES = 304


T = TypeVar('T')


class SuccessResult(NamedTuple):
	result: T


class FailureResult(NamedTuple):
	code: int


Result = Union[SuccessResult[T], FailureResult]


def as_success(result: Result[T]) -> Optional[T]:
	if isinstance(result, SuccessResult):
		return result.result
	return None


def as_failure(result: Result[T]) -> Optional[int]:
	if isinstance(result, FailureResult):
		return result.code
	return None


# NOW we get to the database interactions

def new_db_id() -> str:
	return nanoid.generate(alphabet='BCDFGHLMNPQRSTXZbcdfghlmnpqrstxz', size=24)


INDEX_NAME_REGEX = re.compile("[0-9a-z_]+")


def validate_index_name(index_name: str) -> Optional[str]:
	if INDEX_NAME_REGEX.fullmatch(index_name):
		return str(index_name)
	return None


def validate_index_type(index_type: str) -> Optional[Literal['sort', 'unique']]:
	valid_indices = {'sort', 'unique'}
	if index_type in valid_indices:
		return str(index_type)
	else:
		return None


def empty_result_to_json(result: Result[None], cmd_id: Optional[int]) -> Any:
	value = {'success': True, 'cmd_id': cmd_id}
	error = as_failure(result)
	if error:
		value = {'success': False, 'error': error, 'cmd_id': cmd_id}
	if cmd_id:
		value['cmd_id'] = cmd_id
	return value


async def create_table(db: AsyncIOMotorDatabase, owner: str, table_spec: Any) -> Result[None]:
	table_name = str(table_spec['name'])
	owner = str(owner)
	# Check if it already exists
	table = await db.tables.find_one({'name': table_name, 'owner': owner})
	if table:
		return FailureResult(int(PhasmaDBErrorCode.TABLE_ALREADY_EXISTS))
	
	indices = dict()
	for (k, v) in table_spec['indices'].items():
		k = validate_index_name(k)
		v = validate_index_type(v)
		if (not k) or (not v):
			return FailureResult(int(PhasmaDBErrorCode.REQUEST_IMPROPERLY_FORMATTED))
		indices[k] = v
	
	new_table = {
		'_id': new_db_id(),
		'name': table_name,
		'owner': owner,
		'indices': indices
	}
	await db.tables.insert_one(new_table)
	
	collection_name = f'{owner}_{table_name}'
	index_creations = [db[collection_name].create_index(f"row_id", unique=True)]
	for (k, v) in indices.items():
		if v == 'sort':
			index_creations.append(db[collection_name].create_index(f"index.{k}"))
		elif v == 'unique':
			index_creations.append(db[collection_name].create_index(f"index.{k}", unique=True))
	
	await asyncio.gather(*index_creations)
	
	return SuccessResult(None)


async def insert_datum(db: AsyncIOMotorDatabase, owner: str, table: Any, datum_id: str, datum: dict) -> tuple[str, Result[None]]:
	datum_id = str(datum_id)
	collection_name = f"{owner}_{table['name']}"
	table_indices = table['indices']
	
	any_with_id = await db[collection_name].find_one(datum_id)
	if any_with_id:
		return datum_id, FailureResult(int(PhasmaDBErrorCode.ROW_SAME_ID_ALREADY_EXISTS))
	
	indexed_data = {}
	test_unique_indices = []
	for (index_name, index_type) in table_indices.items():
		if index_name not in datum['indexed'].keys():
			return datum_id, FailureResult(int(PhasmaDBErrorCode.ROW_LACKS_SOME_INDEXED_VALUES))
		
		row_index_value = int(datum['indexed'][index_name])
		indexed_data[index_name] = row_index_value
		if index_type == 'unique':
			test_awaitable = db[collection_name].find_one({f'index.{index_name}': row_index_value})
			test_unique_indices.append(test_awaitable)
	
	if any(k not in indexed_data.keys() for k in datum['indexed']):
		return datum_id, FailureResult(int(PhasmaDBErrorCode.ROW_HAS_EXTRA_INDEXED_VALUES))
	
	test_unique_index_results = await asyncio.gather(*test_unique_indices)
	if any((r is not None) for r in test_unique_index_results):
		return datum_id, FailureResult(int(PhasmaDBErrorCode.ROW_SAME_UNIQUES_ALREADY_EXISTS))
	
	await db[collection_name].insert_one({'_id': new_db_id(), 'row_id': datum_id, 'index': indexed_data, 'extra': str(datum['extra'])})
	return datum_id, SuccessResult(None)


async def insert_data(db: AsyncIOMotorDatabase, owner: str, table_name: str, data: Dict[str, dict]) -> Dict[str, Result[None]]:
	table_name = str(table_name)
	owner = str(owner)
	# Check if it already exists
	table = await db.tables.find_one({'name': table_name, 'owner': owner})
	if not table:
		return {k: FailureResult(int(PhasmaDBErrorCode.TABLE_DOES_NOT_EXIST)) for k in data.keys()}
	
	insertions = []
	for (datum_id, datum) in data.items():
		insertions.append(insert_datum(db, owner, table, datum_id, datum))
	
	return {k: v for (k, v) in await asyncio.gather(*insertions)}


async def process_command(db: AsyncIOMotorDatabase, user: str, command: Any) -> Any:
	if command['cmd'] == 'create_table':
		result = await create_table(db, user, command)
		return empty_result_to_json(result, command['cmd_id'])
	elif command['cmd'] == 'insert_data':
		results = await insert_data(db, user, command['table'], command['data'])
		return {'cmd_id': command['cmd_id'], 'results': {k: empty_result_to_json(v, None) for (k, v) in results.items()}}
	else:
		return {'success': False, 'error': int(PhasmaDBErrorCode.COMMAND_TYPE_DOES_NOT_EXIST)}
