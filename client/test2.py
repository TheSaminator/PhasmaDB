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
		
		delete_by_id_result = await phasma.delete_by_id(my_keyring, 'officers', 'row 1', on_error=print_err)
		if delete_by_id_result:
			print("Done deleting by id")
		else:
			print("Deleting by id failed")
		
		delete_by_text_result = await phasma.delete_data(my_keyring, 'officers', phasmadb.Column('officer_email') == "dgsf@zoomer.union", on_error=print_err)
		if delete_by_text_result:
			print("Done deleting by text index:")
			print(delete_by_text_result)
		else:
			print("Deleting by text index failed")
		
		delete_result = await phasma.delete_data(my_keyring, 'officers', phasmadb.Column('officer_rank') < 2, on_error=print_err)
		if delete_result:
			print("Done deleting by sort index:")
			print(delete_result)
		else:
			print("Deleting by sort index failed")
		
		delete_all_result = await phasma.delete_data(my_keyring, 'officers', phasmadb.SelectAll, on_error=print_err)
		if delete_all_result:
			print("Done deleting everything:")
			print(delete_all_result)
		else:
			print("Deleting everything failed")
		
		drop_table_result = await phasma.drop_table(my_keyring, 'officers', on_error=print_err)
		if drop_table_result:
			print("Done dropping table")
		else:
			print("Dropping table failed")
		
		await phasma.close()
		print("Done closing connection")
		
		await connection


if __name__ == '__main__':
	asyncio.run(main())
