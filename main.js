'use strict';

const events = require('events'),
	util = require('util'),
	{Connection, Request, TYPES} = require('tedious');


// used to match type strings in parameter definitions
const TYPEHASH = {};
for(var k in TYPES) {
	if(TYPES.hasOwnProperty(k)) {
		TYPEHASH[k.toLowerCase()] = TYPES[k];
	}
}


function _impl(args) {
	const self = this;
	
	// load connection / tedious config
	// will decode username, password, server ip from base64 obfuscation
	// note, none of these settings are encrypted
	const sqlcfg = objectAssign({}, args);
	if(sqlcfg.authentication.options.userName.startsWith('data:')) sqlcfg.authentication.options.userName = atob(sqlcfg.authentication.options.userName.replace('data:',''));
	if(sqlcfg.authentication.options.password.startsWith('data:')) sqlcfg.authentication.options.password = atob(sqlcfg.authentication.options.password.replace('data:',''));
	if(sqlcfg.server.startsWith('data:')) sqlcfg.server = atob(sqlcfg.server.replace('data:',''));
	
	// PRIVATE VARS
	var connection;
	var closing = false, attemptCount = 0;
	
	
	function handleConnection(err) {
		if(err) {
			if(attemptCount < 10) setTimeout(getConnection, (++attemptCount) * 5000);
			
			console.log('Connection error.');
			self.emit('connectFail', attemptCount < 10);
			return;
		}
		
		attemptCount = 0;
		self.emit('connected');
	}
	
	function handleConnectionClose() {
		self.emit('connectClose');
		
		// handle connection unexpectedly closed
		if(!closing) {
			// start retry loop
			if(attemptCount == 0) {
				getConnection();
			}
		}
	}
	
	
/*
	DUMMY LOG FUNCTIONS
*/
	function logServerMessage(mesgObj) {
		console.log(mesgObj);
	}
	
	function logError(err) {
		console.log(err);
	}
	
	function logDebug(mesg) {
		console.log(mesg);
	}
	
	
	
/*
	TEDIUOUS - Connect and attach to loggers
*/
	
	function getConnection() {
		// connect
		connection = new Connection(sqlcfg);
		
		connection.on('connect', handleConnection);
		connection.on('end', handleConnectionClose);
		connection.on('error', logError);
		connection.on('debug', logDebug);
		connection.on('infoMessage', logServerMessage);
		connection.on('errorMessage', logServerMessage);
	};
	
	
	
	
/*
	PUBLIC API
*/
	
	// convenience method - passes query directly to Tedious
	this.exec = function(sql, callback) {
		connection.execSql(new Request(sql, (callback || function() {})));
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
					var req = new Request(sql, (err, rowCount, rows) => {
						if(err) return console.log(err);
						return resolve(rowCount > 0 ? rows : []);
					});
					
					// cycle through params and set up
					for(var i = 0; i < params.length; i++) {
						var x = req.addParameter(params[i].name, TYPEHASH[params[i].type.toLowerCase()], obj[params[i].name], params[i].options);
						console.log(x);
					}
					
					connection.execSql(req);
				});
			};
		}
		
		
		return function(obj, optcallback) {
			obj = obj || {};
			var req = new Request(sql, optcallback || callback);
			
			//console.log(sql);
			
			// cycle through params and set up
			for(var i = 0; i < params.length; i++) {
				var x = req.addParameter(params[i].name, TYPEHASH[params[i].type.toLowerCase()], obj[params[i].name], params[i].options);
				console.log(x);
			}
			
			console.log(req);
			
			connection.execSql(req);
		};
		
	};
	
	
	
	// establish connection immediately
	getConnection();
}

util.inherits(_impl, events);

module.exports = _impl;



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