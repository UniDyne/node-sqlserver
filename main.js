'use strict';

const {Connection, Request, TYPES} = require('tedious');


// used to match type strings in parameter definitions
const TYPEHASH = {};
for(var k in TYPES) {
	if(TYPES.hasOwnProperty(k)) {
		TYPEHASH[k.toLowerCase()] = TYPES[k];
	}
}


const
	NEW = 0,
	IDLE = 1,
	BUSY = 2,
	
	MAX_POOL = 16;
	
	
function SQLServerPool(config) {
	
	// load connection / tedious config
	// will decode username, password, server ip from base64 obfuscation
	// note, none of these settings are encrypted
	const sqlcfg = objectAssign({
		server: '127.0.0.1',
		authentication: {
			options: {
				userName: '',
				password: ''
			}
		}
	}, config);
	if(sqlcfg.authentication.options.userName.startsWith('data:')) sqlcfg.authentication.options.userName = atob(sqlcfg.authentication.options.userName.replace('data:',''));
	if(sqlcfg.authentication.options.password.startsWith('data:')) sqlcfg.authentication.options.password = atob(sqlcfg.authentication.options.password.replace('data:',''));
	if(sqlcfg.server.startsWith('data:')) sqlcfg.server = atob(sqlcfg.server.replace('data:',''));
	
	
	
	const pool = [], requestQueue = [], self = this;
	var running = true, queueProc;
	
	
	// create connection
	function createConnection(config) {
		if(!running) return null;
		
		var connection = new Connection(sqlcfg);
		
	//	connection.on('connect', handleConnection);
	//	connection.on('end', handleConnectionClose);
		connection.on('error', logError);
	//	connection.on('debug', logDebug);
	//	connection.on('infoMessage', logServerMessage);
	//	connection.on('errorMessage', logServerMessage);
		
		return connection;
	}

	function acquireConnection() {
		return new Promise((resolve) => {
			if(!running) resolve(null);
			
			// find and return idle connection
			for(var i = 0; i < pool.length; i++) {
				if(pool[i].status == IDLE) {
					return resolve(pool[i]);
				}
			}
			
			// or attempt to create a new connection
			if(pool.length < MAX_POOL) {
				var con = createConnection(sqlcfg);
				if(con == null) resolve(null);
				
				var entry = {
					con: con,
					status: NEW
				};
				
				// when connection made, return to caller
				con.on('connect', (err) => {
					if(err) return handleError(err);
					entry.status = IDLE;
					return resolve(entry);
				});
				
				// if connection ends, remove from pool
				con.on('end', () => {
					for(var i = 0; i < pool.length; i++) {
						if(pool[i] === entry)
							return pool.splice(i,1);
					}
				});
				
				pool.push(entry);
			} else return resolve(null);
			// no connections available, return null
		});
	}
	
	
	async function queueProc() {
		if(!running) return;
		
		console.log(`Queue Length: ${requestQueue.length}     Pool: ${pool.length}`);
		if(requestQueue.length > 0) {
			var pooled = await acquireConnection();
			if(pooled != null) {
				var job = requestQueue.shift();
				job(pooled);
			}
		}
		
		if(running) {
			if(requestQueue.length > 0) setImmediate(queueProc);
			else setTimeout(queueProc, 500);
		}
	}
	
	
	this.start = function() {
		running = true;
		return setImmediate(queueProc);
	};
	
	this.stop = function() {
		running = false;
	};
	
	
	// convenience method - passes query directly to Tedious
	this.exec = async function(sql, callback) {
		requestQueue.push((pooled) => {
			pooled.status = BUSY;
			pooled.con.execSql(new Request(sql, (err, count, rows) => {
				pooled.status = IDLE;
				if(callback) return callback(err, count, rows);
			}));
		});
	};
	
	
	
	// load an array of query definitions
	// returns an object with function calls for each named query
	this.loadQueries = function(queryList) {
		var queryHash = {};
		
		for(var i = 0; i < queryList.length; i++) {
			queryHash[queryList[i].id] = self.loadQuery(queryList[i].sql, queryList[i].params || null, queryList[i].callback || null, queryList[i].usePromise ? true : false);
		}
		
		return queryHash;
	};
	
	
	// load a single query definition
	// returns a function for calling the query
	// if usePromise is true, will return a promise-style function
	// rather than a callback-style one
	this.loadQuery = function(sql, params, callback, usePromise) {
		if(!callback) callback = function() {};
		if(!params) params = [];
		
		if(usePromise) {
			return function(obj) {
				obj = obj || {};
				return new Promise(resolve => {
					
					requestQueue.push((pooled) => {
						pooled.status = BUSY;
						
						var req = new Request(sql, (err, rowCount, rows) => {
							if(err) return console.log(err);
							pooled.status = IDLE;
							return resolve(rowCount > 0 ? rows : []);
						});
						
						// cycle through params and set up
						for(var i = 0; i < params.length; i++) {
							req.addParameter(params[i].name, TYPEHASH[params[i].type.toLowerCase()], obj[params[i].name], params[i].options);
						}
						
						pooled.con.execSql(req);
					});
				});
			};
		}
		
		return function(obj, optcallback) {
			requestQueue.push((pooled) => {
				obj = obj || {};
				
				pooled.status = BUSY;
				
				var req = new Request(sql, (err, rowCount, rows) => {
					pooled.status = IDLE;
					return (optcallback || callback)(err, rowCount, rows);
				});
				
				//console.log(sql);
				
				// cycle through params and set up
				for(var i = 0; i < params.length; i++) {
					req.addParameter(params[i].name, TYPEHASH[params[i].type.toLowerCase()], obj[params[i].name], params[i].options);
				}
				
				console.log(`   Request: ${req}`);
				
				pooled.con.execSql(req);
			});
		};
		
	};
	
	
	return this;
}

function logError(e) {
	console.log(`SQL Error: ${e}`);
}



module.exports = {
	SQLServerPool
};




// Object.assign polyfill
function objectAssign(target, source) {
	var from;
	var to = Object(target);
	var symbols;

	for (var s = 1; s < arguments.length; s++) {
		from = Object(arguments[s]);

		for (var key in from) {
			if (hasOwnProperty.call(from, key)) {
				to[key] = from[key];
			}
		}

		if (Object.getOwnPropertySymbols) {
			symbols = Object.getOwnPropertySymbols(from);
			for (var i = 0; i < symbols.length; i++) {
				if (propIsEnumerable.call(from, symbols[i])) {
					to[symbols[i]] = from[symbols[i]];
				}
			}
		}
	}

	return to;
}
