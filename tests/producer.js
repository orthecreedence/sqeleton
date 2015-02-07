var sqeleton = require('../clients/node/client');

var jobs = 9999;
var i = 0;
var loop = function()
{
	console.log('queue job', i);
	sqeleton.enqueue('jobs', '{"name":"do-stuff"}', 10, 128, 0)
		.then(function(res) {
			console.log('res: ', res);
		});

	i++;
	if(i < jobs) setTimeout(loop);
};

for(var i = 0; i < 10; i++)
{
	loop();
}

