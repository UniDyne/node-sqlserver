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
	
	FAIL = 3,
	
	MAX_POOL = 16;



function handleError(err) {
	if(global.output) global.output.error(JSON.stringify(err));
	else console.log(JSON.stringify(err));
}


function setParameters(req, params, obj) {
	for(var i = 0; i < params.length; i++) {
		req.addParameter(params[i].name, TYPEHASH[params[i].type.toLowerCase()], obj[params[i].name], params[i].options);
	}
}


function flattenResults(rows) {
	var rr = [];
	for(var i = 0; i < rows.length; i++) {
		rr.push(Object.entries(rows[i]).reduce((a,v) => {a[v[0]] = v[1].value; return a;}, {}));
	}
	return rr;
}

	
	
function SQLServerPool(config) {
	
	// load connection / tedious config
	// will decode username, password, server ip from base64 obfuscation
	// note, none of these settings are encrypted
	const sqlcfg = Object.assign({
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
		
		return new Connection(sqlcfg);
	}

	function acquireConnection() {
		return new Promise((resolve) => {
			if(!running) resolve(null);
			
			// find and return idle connection
			for(var i = pool.length - 1; i >= 0; i--) {
				if(pool[i].status == IDLE) {
					return resolve(pool[i]);
				}

				// remove broken connections
				if(pool[i].status == FAIL) {
					var broken = pool.splice(i,1);
					broken.con.close();
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

				con.on('error', (err) => {
					//console.log('aquireConnection()');
					handleError(err);
					entry.status = FAIL;
					return resolve(null);
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
		
		//console.log(`Queue Length: ${requestQueue.length}     Pool: ${pool.length}`);
		if(requestQueue.length > 0) {
			var pooled = await acquireConnection();
			if(pooled != null) {
				var job = requestQueue.shift();

				pooled.con.removeAllListeners('socketError');
				pooled.con.on('socketError', (err) => {
					console.log('queueProc()');
					handleError(err);
					pooled.status = FAIL;
					job.pooled = null;
					requestQueue.push(job);
					pooled.con.removeAllListeners('socketError');
					queueTimer = 2000;
				});

				job.pooled = pooled;
				execJob(job);
			}
		}
		
		if(running) {
			//if(requestQueue.length > 0) setImmediate(queueProc);
			//else setTimeout(queueProc, 500);
			setTimeout(queueProc, queueTimer);
			if(requestQueue.length > 0) queueTimer = Math.max(10, queueTimer - 10);
			else queueTimer = Math.min(100, queueTimer + 10);
		}
	}
	

	function createCallbackQuery(queryDef) {
		return function(obj, optcallback) {
			obj = obj || {};
			const job = {
				queryDef: queryDef,
				params: obj,
				callback: optcallback || queryDef.callback,
				pooled: null
			};
			job.resultHandler = getCallbackHandler(job);
			requestQueue.push(job);
		}
	}
	
	function getCallbackHandler(job) {
		if(!queryDef.callback) queryDef.callback = ()=>{};
		return (err, rowCount, rows) => {
			job.pooled.status = IDLE;
			return job.callback(err, rowCount, rows);
		}
	}
	
	
	function createPromiseQuery(queryDef) {
		return function(obj) {
			obj = obj || {};
			return new Promise((resolve, reject) => {
				const job = {
					queryDef: queryDef,
					params: obj,
					resolve: resolve,
					reject: reject,
					pooled: null
				};
				job.resultHandler = getPromiseHandler(job);
				requestQueue.push(job);
			});
		}
	}
	
	function getPromiseHandler(job) {
		return (err, rowCount, rows) => {
			if(err) {
				handleError(err);
				// if timeout occurred, need to requeue
				if(err.code == "ETIMEOUT" || err.code == "ESOCKET") {
					job.pooled.status = FAIL;
					job.pooled = null;
					return requestQueue.push(job);
				} else return job.reject();
			}
			
			job.pooled.status = IDLE;
			
			if(rowCount == 0) return job.resolve([]);
			
			if(!job.queryDef.flatten) return job.resolve(rows);
			
			return job.resolve(flattenResults(rows));
		}
	}
	
	
	function execJob(job) {
		job.pooled.status = BUSY;
		const req = new Request(job.queryDef.sql, job.resultHandler);
		setParameters(req, job.queryDef.params, job.params);
		job.pooled.con.execSql(req);
	}

	
	this.start = function() {
		running = true;
		return setImmediate(queueProc);
	};
	
	this.stop = function() {
		running = false;
	};
	
	this.exit = function() {
		for(var i = 0; i < pool.length; i++) {
			pool[i].con.close();
		}
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
		
		// default basedir is the one above node_modules
		if(!baseDir) baseDir = path.join(__dirname, '..', '..');
		
		for(var i = 0; i < queryList.length; i++) {
			// if sql starts with colon, load the query from a file
			if(queryList[i].sql.substr(0,1) == ':')
				queryList[i].sql = fs.readFileSync(path.join(baseDir, queryList[i].sql.substr(1)), 'utf8');
			
			if(!queryList[i].params) queryList[i].params = [];
			
			if(queryList[i].usePromise) queryHash[queryList[i].id] = createPromiseQuery(queryList[i]);
			else queryHash[queryList[i].id] = createCallbackQuery(queryList[i]);
		}
		
		return queryHash;
	};
	
	
	// load a single query definition
	// returns a function for calling the query
	// if usePromise is true, will return a promise-style function
	// rather than a callback-style one
	/** @deprecated **/
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


module.exports = {
	SQLServerPool
};

