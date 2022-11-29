from enum import IntEnum
from typing import Any, NamedTuple, Generic, TypeVar, Optional, Union, Literal

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
def validate_index_type(index_type: str) -> Union[Literal['add', 'multiply', 'sort', 'unique'], None]:
	valid_indices = {'add', 'multiply', 'sort', 'unique'}
	if index_type in valid_indices:
		return str(index_type)
	else:
		return None


async def create_table(db: AsyncIOMotorDatabase, owner: str, table_spec: Any) -> Result[None]:
	table_name = str(table_spec['name'])
	owner = str(owner)
	# Check if it already exists
	table = await db.tables.find_one({'name': table_name, 'owner': owner})
	if table:
		return FailureResult(int(PhasmaDBErrorCode.TABLE_ALREADY_EXISTS))
	
	indices = dict()
	for (k, v) in table_spec['indices'].items():
		v = validate_index_type(v)
		if not v:
			return FailureResult(int(PhasmaDBErrorCode.REQUEST_IMPROPERLY_FORMATTED))
		indices[str(k)] = v
	
	new_table = {
		'name': table_name,
		'owner': owner,
		'indices': indices
	}
	await db.tables.insert_one(new_table)
	
	collection_name = f'{owner}_{table_name}'
	index_creations = []
	for (k, v) in indices.items():
		if v == 'sort':
			index_creations.append(db[collection_name].create_index(k))
		elif v == 'unique':
			index_creations.append(db[collection_name].create_index(k, unique=True))
	
	for index_creation in index_creations:
		await index_creation
	
	return SuccessResult(None)


async def process_command(db: AsyncIOMotorDatabase, user: str, command: Any) -> Any:
	if command['cmd'] == 'create_table':
		result = await create_table(db, user, command)
		error = as_failure(result)
		if error:
			return {'success': False, 'error': error}
		else:
			return {'success': True, 'cmd_id': command['cmd_id']}
	else:
		return {'success': False, 'error': int(PhasmaDBErrorCode.COMMAND_TYPE_DOES_NOT_EXIST)}
