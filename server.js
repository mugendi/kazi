var express = require('express');
var path = require('path');
var _ = require('lodash');
var ip = require('ip');
var ip = ip.address();

var Queue=require('./lib/queue.js');

var options={
	// maxClients:100,
	// timeoutLazyClientsFor:3600,
	// expireClientsAfter:timeoutLazyClientsFor/1000,
	strictFIFO:true,
	rescheduleStuckJobsAfter:30
}

var queue = new Queue(options);




var app = express();

var bodyParser = require('body-parser')
app.use( bodyParser.json() );       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded({     // to support URL-encoded bodies
  extended: true
})); 



// inside middleware handler 
var ipMiddleware = function(req, res, next) {
    clientIp = requestIp.getClientIp(req); // on localhost > 127.0.0.1 
    next();
};


[
	'jobUpdated',
	'jobQueued',
	'jobFinished',
	'jobsCleared',
	'jobType',
	'jobRescheduled',
	'clientServed',
	'clientNotServed',
	'clientJobRequest',
	'clientListed',
	'clientFreed',
	'clientBusy',
	'clientExists'
]
.forEach(function(evt){

	queue.on(evt, function(msg,client,job){
		if(!job){
			job=_.clone(client);
		}

		event_msg(msg,client,job);
	});

});

function event_msg(msg,client,job){
	// console.log(msg,client,job);

	if(!_.isUndefined(job) && !_.isUndefined(job.id)){
		console.log(' > '+ msg + ( !_.isUndefined(job.id) ? ' [JOB:ID '+ job.id +']': ''));
	}
	else{

		// console.log(job);
		console.log(' > '+ msg );
	}
}



app.set('port', process.env.PORT || 2016);

app.get('/' , function(req, res) {
	res.end('sss')
});

app.get('/listclient' , function(req, res) {

	// console.log(req.query);

	var client={
		ip:ip,
		name:req.query.name
	}


	//listClient
	queue.listClient(client,function(err,client){

		res.send(client);
	});

});


app.post('/queuejob' , function(req, res) {

	// console.log(_.values(req.body));

	queue.queueJob(_.values(req.body),false,function(err,jobs){

		if(err){
			res.send({});
		}
		else{
			res.send(jobs);
		}
		
	});


});


app.post('/requestjob' , function(req, res) {
	
	//listClient
	queue.requestJob(req.body,function(err,job){
		
		if(err){
			res.send({});			
		}
		else{
			res.send(job);
		}
		
	});


});

app.post('/finishJob' , function(req, res) {

	//job finished, remove
	queue.finishJob(req.body.client,req.body.job,function(err,deleted){
		// res.send(job);
		res.send(deleted?{msg:'Success',code:200}:{msg:'Error',code:400});
	});

});

app.post('/updateJob' , function(req, res) {
	//update job
	queue.updateJob(req.body.client,req.body.job,req.body.result,function(err,updated){
		res.send(updated?{msg:'Success',code:200}:{msg:'Error',code:400});
	});

});


/*
	start listening...
*/
app.listen(app.get('port'), function() {
  console.log('Express server listening on port ' + app.get('port'));
});

