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
		
		await phasma.create_table(my_keyring, 'officers', {
			'officer_number': 'unique',
			'officer_rank': 'sort',
			'officer_email': 'unique_text'
		}, on_error=print_err)
		print("Done creating table")
		
		await phasma.insert_data(my_keyring, 'officers', {
			'row 1': phasmadb.PhasmaDBDataRow({
				'officer_number': 1914,
				'officer_rank': 4,
				'officer_email': "chadmusket@zoomer.union"
			}, {
				'officer_name': "Gaius Patallius Vanesco",
				'officer_email': "chadmusket@zoomer.union"
			}),
			'row 2': phasmadb.PhasmaDBDataRow({
				'officer_number': 8570,
				'officer_rank': 3,
				'officer_email': "dgsf@zoomer.union"
			}, {
				'officer_name': "Marcus Colimarnius Iacomus",
				'officer_email': "dgsf@zoomer.union"
			}),
			'row 3': phasmadb.PhasmaDBDataRow({
				'officer_number': 2247,
				'officer_rank': 2,
				'officer_email': "laniustrolling@zoomer.union"
			}, {
				'officer_name': "Legatus Lanius Trollator",
				'officer_email': "laniustrolling@zoomer.union"
			}),
			'row 4': phasmadb.PhasmaDBDataRow({
				'officer_number': 1377,
				'officer_rank': 1,
				'officer_email': "fortniteluvr69@zoomer.union"
			}, {
				'officer_name': "Lucius Denallius Valca",
				'officer_email': "fortniteluvr69@zoomer.union"
			})
		}, on_error=print_err)
		print("Done inserting data")
		
		query_by_id_result = await phasma.query_by_id(my_keyring, 'officers', 'row 4', ['officer_number', 'officer_rank'], on_error=print_err)
		if query_by_id_result:
			print("Done querying by id:")
			print(repr(query_by_id_result))
		else:
			print("Querying by id failed")
		
		query_result_1 = await phasma.query_data(my_keyring, 'officers', phasmadb.PhasmaDBDataQuery(
			select=phasmadb.Column('officer_email') == 'dgsf@zoomer.union',
			sort=[]
		), ['officer_number', 'officer_rank'], on_error=print_err)
		if query_result_1:
			print("Done querying data by email:")
			print(repr(query_result_1))
		else:
			print("Querying data by email failed")
		
		query_result_2 = await phasma.query_data(my_keyring, 'officers', phasmadb.PhasmaDBDataQuery(
			select=phasmadb.Column('officer_rank') > 1,
			sort=[('officer_rank', 'desc')]
		), ['officer_number', 'officer_rank'], on_error=print_err)
		if query_result_2:
			print("Done querying data by rank:")
			print(repr(query_result_2))
		else:
			print("Querying data by rank failed")
		
		await phasma.close()
		print("Done closing connection")
		
		await connection


if __name__ == '__main__':
	asyncio.run(main())
