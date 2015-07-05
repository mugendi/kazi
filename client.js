
var path=require('path')
var client=require('./lib/workers.js');

var workers_path=path.join(__dirname,'workers');
var jobIds=['categoright.*','twitter.*']


client.start(workers_path,jobIds)