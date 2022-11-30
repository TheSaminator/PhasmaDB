# Client-Server Interactions

Every client command has a type under the `"cmd"` key, and an id under the `"cmd_id"` key. The id is used to
keep track of the command so the server can respond to it, and the client can know what command the server is
responding to.

Every server response has a `"cmd_id"` field, too, which is the id of the client command that the server is
responding to.

`"cmd_id"` is up to client discretion, but our implementation of the client library will use auto-incrementing ids.

## Error codes

* Request-related
    * 1: Command type does not exist
    * 2: Request is improperly formatted
* Login-related
    * 101: User does not exist
    * 102: Authentication bytes did not match
* Table-related
    * 201: Table does not exist
    * 202: Table already exists
* Row-related
    * 301: Row with same ID already exists
    * 302: Row with same unique value already exists
    * 303: Not all indexed columns have values
    * 304: Extra values specified for non-existent indices

## Handshake

**(1) Client login**: Client wants to log in as CoolDude69  
`{"username": "CoolDude69"}`

**(2a) Server cannot find user**: Server encrypts random bytes with CoolDude69's public key and sends it to client for decryption  
`{"challenge": null, "error": 101}`

**(2b) Server challenge**: Server encrypts random bytes with CoolDude69's public key and sends it to client for decryption  
`{"challenge": "<some random bytes in hex format, encrypted with CoolDude69's public key>"}`

**(3) Client response to challenge**: Client decrypts these bytes with their private key and sends them back to the server  
`{"response": "<those same random bytes, decrypted with private key, in hex format>"}`

**(4a) Server verification result (success)**: The bytes match, the client is now logged in as CoolDude69  
`{"success": true}`

**(4b) Server verification result (failure)**: The bytes don't match, authentication has failed  
`{"success": false, "error": 102}`

## Table creation

**Table creation request format**

```json
{
	"cmd": "create_table",
	"cmd_id": 1,
	"name": "<name of table>",
	"indices": {
		"<a column i want to index>": "add",
		"<another column i want to index>": "multiply",
		"<a third column i want to index>": "sort",
		"<a column i want to keep unique>": "unique"
	}
}
```

The client library will handle hashing the names of indexed columns with the name of the table

**Table creation response format: success**

```json
{
	"cmd_id": 1,
	"success": true
}
```

**Table creation response format: failure**

```json
{
	"cmd_id": 1,
	"success": false,
	"error": 202
}
```

The only possible error from creating a table is when the user tries
to create a table with the same name as one that already exists.

## Data insertion

**Row insertion request format**

```json
{
	"cmd": "insert",
	"cmd_id": 2,
	"table": "<table to insert into>",
	"data": {
		"<row id 1>": {
			"indexed": {
				"<column indexed for sorting>": "<data encrypted with OPE>",
				"<column indexed for addition>": "<data encrypted with Paillier>",
				"<column indexed for multiplying>": "<data encrypted with RSA>",
				"<more indexed columns...>": "<more specifically-encrypted data...>"
			},
			"extra": "<any arbitrary string encrypted with AES/CBC>"
		},
		"<row id 2>": "<same format as above row>",
		"<more rows...>": "<more row data...>"
	}
}
```

**Row insertion response format - table exists, some succeeded, some failed**

```json
{
	"cmd_id": 2,
	"results": {
		"<row id 1>": {
			"success": true
		},
		"<row id 2>": {
			"success": false,
			"error": 301
		},
		"<row id 3>": {
			"success": false,
			"error": 303
		}
	}
}
```

**Row insertion response format - does not exist**

```json
{
	"cmd_id": 2,
	"results": {
		"<each and every single row in the attempted insertion>": {
			"success": false,
			"error": 201
		}
	}
}
```

## Disconnection

The connection may be safely disconnected as so:

**Client -> server**  
`{"cmd": "exit", "cmd_id": 69}`

The server will wait for every awaiting operation to complete, then it will send the following message:

**Server -> client**
`{"cmd_id": 69, "farewell": true}`

Then the connection will be closed.
