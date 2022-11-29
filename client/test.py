import asyncio

import aiohttp

import clientlib


async def main():
	my_keyring = clientlib.PhasmaDBLocalKeyring.create()
	print("Done generating keyring")
	
	async with aiohttp.ClientSession() as session:
		phasma = clientlib.PhasmaDBConnection('http://localhost:8080/phasma-db', clientlib.PhasmaDBLoginCredential.load("test_private.json"), session)
		connection = asyncio.create_task(phasma.loop())
		
		await phasma.create_table(my_keyring, 'my_stuff', {})
		print("Done creating table")
		
		await phasma.close()
		print("Done closing connection")
		
		await connection


if __name__ == '__main__':
	asyncio.run(main())
