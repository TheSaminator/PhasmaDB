import asyncio
import os.path

import aiohttp

import phasmadb


def print_err(operation: str, error: phasmadb.PhasmaDBError, target: tuple[str, ...]):
	print(f"Error in executing operation {operation} for target object {target}: {repr(error)}")


async def main():
	if not os.path.isfile('test_keyring.json'):
		with open('test_keyring.json', 'w') as f:
			f.write(phasmadb.PhasmaDBLocalKeyring.create().save())
			print("Done generating keyring")
	
	with open('test_keyring.json') as f:
		my_keyring = phasmadb.PhasmaDBLocalKeyring.load(f.read())
		print("Done loading keyring")
	
	async with aiohttp.ClientSession() as session:
		phasma = phasmadb.PhasmaDBConnection()
		connection = asyncio.create_task(phasma.connection('http://localhost:8080/phasma-db', phasmadb.PhasmaDBLoginCredential.load("test_private.json"), session))
		
		await phasma.delete_by_id(my_keyring, 'officers', 'row 1', on_error=print_err)
		
		delete_by_text_result = await phasma.delete_data(my_keyring, 'officers', phasmadb.Column('officer_email') == "dgsf@zoomer.union", on_error=print_err)
		if delete_by_text_result:
			print("Done deleting by text index:")
			print(delete_by_text_result)
		else:
			print("Deleting by text index failed")
		
		delete_result = await phasma.delete_data(my_keyring, 'officers', phasmadb.Column('officer_rank') < 3, on_error=print_err)
		if delete_result:
			print("Done deleting by sort index:")
			print(delete_result)
		else:
			print("Deleting by sort index failed")
		
		await phasma.drop_table(my_keyring, 'officers', on_error=print_err)
		print("Done dropping table")
		
		await phasma.close()
		print("Done closing connection")
		
		await connection


if __name__ == '__main__':
	asyncio.run(main())
