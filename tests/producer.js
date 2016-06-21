var Promise = require('../clients/node/node_modules/bluebird');
var sqeleton = require('../clients/node/').default;

var jobs = 10000;
var num_workers = 20;
var i = 0;
var worker = function()
{
	return new Promise(function(resolve, reject) {
		var loop = function()
		{
			if(i >= jobs) return resolve();
			i++;
			sqeleton.enqueue('jobs', '{"name":"do-stuff"}', 10, 128, 0)
				.then(function(res) {
					if(res % 1000 == 0) console.log('res: ', res);
					setImmediate(loop);
				})
				.catch(function(e) {
					i--;
					throw e;
				});

		};
		loop();
	});
};


var actions = [];
for(var i = 0; i < num_workers; i++)
{
	actions.push(worker());
}

Promise.all(actions)
	.finally(function() {
		return sqeleton.close();
	});

