import argparse
import base64
import json
import os
from typing import TextIO

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption

from auth import USERNAME_REGEX


def generate_user(username: str, private_key_file: TextIO):
	if not USERNAME_REGEX.fullmatch(username):
		print("Error: Invalid username - must contain only letters, digits, and underscores")
		return
	
	rsa_private_key = rsa.generate_private_key(public_exponent=65537, key_size=4096)
	rsa_public_key = rsa_private_key.public_key()
	
	if not os.path.isdir('public_keys'):
		os.mkdir("public_keys")
	
	with open(f'public_keys/{username}.pem', 'wb') as public_key_file:
		public_key_file.write(rsa_public_key.public_bytes(Encoding.PEM, PublicFormat.PKCS1))
	
	privkey = {'username': username, 'private_key': base64.b64encode(rsa_private_key.private_bytes(Encoding.PEM, PrivateFormat.PKCS8, NoEncryption())).decode('ascii')}
	private_key_file.write(json.dumps(privkey))
	private_key_file.close()
	
	print(f"Done! Public key is stored in public_keys/{username}.pem, private key has been written to {private_key_file.name}")


if __name__ == '__main__':
	parser = argparse.ArgumentParser(
		prog='new_user',
		description="Creates a new user for this PhasmaDB installation"
	)
	parser.add_argument(
		"username",
		help="The user's name that will be used to log in - letters, digits, and underscores only"
	)
	parser.add_argument(
		"private_key_file",
		help="The file to which the user's private key will be saved - keep this very secret!",
		type=argparse.FileType('w', encoding='utf-8')
	)
	
	args = parser.parse_args()
	generate_user(args.username, args.private_key_file)
