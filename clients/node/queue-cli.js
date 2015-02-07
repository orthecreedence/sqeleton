var argv = require('yargs').argv;

var sqeleton = require('./client.js');

var args = argv._
var cmd = args.shift();
if(!sqeleton[cmd])
{
	console.log('command "'+ cmd +'" not found');
	process.exit();
}
sqeleton[cmd].apply({}, args)
	.then(function(res) {
		console.log('res: ', res);
	})
	.catch(function(err) {
		console.error('client: ', err);
	});

