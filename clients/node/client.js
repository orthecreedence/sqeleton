var redis = require('redis');
var Promise = require('bluebird');
var fs = require('fs');
var crypto = require('crypto');		// for SHA1

var queue = fs.readFileSync(__dirname + '/../../sqeleton.lua');
var script_sha = crypto.createHash('sha1').update(queue).digest('hex');

var commands = {
	enqueue: {
		args: 5,
		pass_ts: true
	},
	dequeue: {
		args: 1,
		pass_ts: true
	},
	delete: {
		args: 1
	},
	release: {
		args: 3,
		pass_ts: true
	},
	bury: {
		args: 2
	},
	kick: {
		args: 2
	},
	wipe: {
		args: 0
	}
};

var process_result = function(res)
{
	if(!Array.isArray(res)) return res

	var obj = {};
	var lastkey = null;
	res.forEach(function(val, i) {
		if(i % 2 == 0)
		{
			lastkey = val;
		}
		else
		{
			if(val.match(/^[0-9]+$/) && lastkey != 'id')
			{
				val = parseInt(val);
			}
			obj[lastkey] = val;
		}
	});

	return obj;
};

exports.client = function(options)
{
	options || (options = {});

	var client = redis.createClient(options.port || 6379, options.host || '127.0.0.1', options);
	client.evalshaAsync = Promise.promisify(client.evalsha);
	client.scriptAsync = Promise.promisify(client.script);

	var exports = {};
	exports.close = client.quit.bind(client);

	Object.keys(commands).forEach(function(cmd) {
		var options = commands[cmd];
		var exports = this;
		exports[cmd] = function()
		{
			var args = [];
			for(var i = 0; i < arguments.length; i++)
			{
				if(i >= options.args) break;
				args.push(arguments[i]);
			}
			return new Promise(function(resolve, reject) {
				client.once('error', reject);
				var unbind = function() { client.removeListener('error', reject); };

				if(args.length < options.args) throw new Error('Wrong number of args passed for '+ cmd);

				if(options.pass_ts) args.push(new Date().getTime());
				args.unshift(cmd);
				args.unshift(0)

				var do_call = function()
				{
					return client.evalshaAsync.apply(client, [script_sha].concat(args))
				};
				return do_call()
					.catch(function(err) {
						if(err.message.match(/Please use EVAL/))
						{
							//console.log('script load');
							return client.scriptAsync('load', queue)
								.then(do_call);
						}
						else
						{
							throw err;
						}
					})
					.then(process_result)
					.then(resolve)
					.catch(reject)
					.finally(unbind);
			});
		};
	}.bind(exports));

	return exports;
};

