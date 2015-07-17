
var path=require('path')
var client=require('./lib/workers.js');

var workers_path=path.join(__dirname,'workers');
var jobIds=['categoright.*','twitter.*','es.*']

var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
// numCPUs=1;
// console.log(numCPUs);



/*//manage multiple clients via cluster...*/
if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  cluster.on('exit', function(worker, code, signal) {    
    console.log('OOps! Worker ' + worker.process.pid + ' died');
    cluster.fork();
  });

} else {

	client.start(workers_path,jobIds);

}



