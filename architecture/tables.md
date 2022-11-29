# Tables' Architecture

Tables use the following information:

* Name of table
* ID column
    * This is not encrypted and should not contain any important data
* Columns w/ indices
    * Sort index: uses order-preserving cipher
    * Unique sort index: also uses order-preserving cipher
    * Add index: uses Paillier cipher
    * Multiply index: uses RSA cipher
* Rest of data: uses AES cipher

## Communication

Everything will be encrypted in transit and in storage
on the server. The client library will encrypt data **before**
sending it, and decrypt data after receiving it.

## Operations

### Create table

* Name of table
* Column indices

Tables may not be modified after creation. To do so,
the table must be deleted and recreated.

Indexed columns must have values in all records.
The rest of a record's (i.e. non-indexed) data may
be in whatever format the client would prefer.

#### Response

**Success:** *no data*  
**Errors:**

* Table already exists

### Insert into table

*Can be either single or bulk*

Each row has the following data:

* Row ID
    * Not encrypted, therefore should **not** contain any sensitive data
* Values for each indexed column
    * Encrypted w/ respective index cipher
        * (Unique) Sort index: order-preserving
        * Add index: Paillier
        * Multiply index: RSA
* Non-indexed data
    * Encrypted w/ AES

#### Response

For each row ID:

**Success:** *no data*  
**Errors:**

* Row with that ID already exists
    * Contains row ID
* Row with that unique column value already exists
    * Contains error column, (encrypted) value, and ID of existing row
* Not all indexed columns have values
    * Contains names of missing columns
* Table does not exist

### Update rows in table

Update contains a filter and a list of modifications

* Filters:
    * ID or set of IDs
    * Value of column w/ sort index within range
        * Endpoint values of range are encrypted w/ order-preserving cipher
    * AND, OR of multiple filters
    * NOT of filter
* Modifications:
    * Set non-indexed data to value
        * Value will be encrypted before sending by client library
        * Uses AES
    * Add to value in column w/ add index
        * Addend will be encrypted before sending by client library
        * Uses Paillier
    * Multiply value in column w/ multiply index
        * Multiplicand will be encrypted before sending by client library
        * Uses RSA

#### Response

**Success:** # of rows affected  
**Errors:**

* Table does not exist

### Delete rows from table

Contains filter, same kind as in update operation,
but no modifications, as the rows are deleted and not updated.

#### Response

**Success:** # of rows deleted  
**Errors:**

* Table does not exist

### Query rows from table

Contains same filter as in update/delete operations,
but no modifications, as the rows are being read from and not written to.

#### Response

**Success:** Data from rows that match filter  
**Errors:**

* Table does not exist

### Drop table

* Name of table

#### Response

**Success:** *no data*  
**Errors:**

* Table does not exist
