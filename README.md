# node-sqlserver

A simple wrapper for the [Tedious](https://www.npmjs.com/package/tedious) driver. This wrapper can turn parameterized queries into function calls that support either the standard callback-style of Tedious or return Promises. The idea is to create an object that encapsulates a list of queries such that you get behaviour similar to a data access object and the particulars of the query are abstracted away.

## Installation
This module is not currently registered with NPM. In order to install, you must use the following command:

`npm install git+https://github.com/unidyne/node-sqlserver.git`

## Usage Example

```js
// load tedious config from a JSON file
const sqlConfig = JSON.parse(fs.readFileSync('./etc/sqlserver.conf'));

// connect to SQL Server using this module
const sqlServer = new require('node-sqlserver')(sqlConfig);

// load queries (could use a JSON file)
const sqlQueries = sqlServer.loadQueries([
	{
		id: "getItems",
		asPromise: true,
		params: [
			{ "name": "id", "type": "int", "options": {} }
		],
		sql: "SELECT id, name FROM items WHERE cat_id = @id"
	},{
		id: "getAllCategories",
		asPromise: false,
		params: [],
		sql: "SELECT id, name FROM category"
	}
]);

// query using callback
sqlQueries.getAllCategories({}, async (err, rowCount, rows) => {
	if(err) return;
	
	for(var i = 0; i < rows.length; i++) {
		
		// query using promise
		var items = await sqlQueries.getItems( rows[i].id.value );
		
		for(var j = 0; j < items.length; j++)
			console.log("Item: " + items[j].name.value);
	}
});
```

# Events
A SQL instance will emit the following events:
* `connectFail` : When the connection dies and cannot be re-established.
* `connected` : When the connection is established or re-established.
* `connectClose` : When connection closes, whether intended or not.

## To Do

* Add single-record call type
* Option to flatten result (no metadata) ?
