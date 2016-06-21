var Promise = require('../clients/node/node_modules/bluebird');
var sqeleton = require('../clients/node/').client();

var jobs_run = 0;
var worker = function()
{
	return new Promise(function(resolve, reject) {
		var next = function()
		{
			sqeleton.dequeue('jobs')
				.then(function(job) {
					if(!job) return resolve();
					jobs_run++;
					if(jobs_run % 1000 == 0) console.log('jobs: ', jobs_run);
					next();
					/*
					sqeleton.delete(job.id)
						.then(next)
						.catch(function(err) {
							console.log('err: ', err);
							setTimeout(next, 1000);
						});
					*/
				})
				.catch(reject);
		};
		next();
	});
};

var actions = [];
for(var i = 0; i < 10; i++) { actions.push(worker()); }

Promise.all(actions)
	.then(function() {
		console.log('all done ', jobs_run);
		sqeleton.close();
	})
	.catch(function(err) {
		console.log('worker err: ', err);
	});



