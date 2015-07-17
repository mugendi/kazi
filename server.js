var express = require('express');
var path = require('path');
var _ = require('lodash');
var ip = require('ip');
var ip = ip.address();
var moment = require('moment');
var config=require('./data/config.json');
var cors = require('cors');


var Queue=require('./lib/queue.js');

var options={
	// maxClients:100,
	// timeoutLazyClientsFor:3600,
	// expireClientsAfter:timeoutLazyClientsFor/1000,
	strictFIFO:true,
	rescheduleStuckJobsAfter:600 //10 mins
}

var queue = new Queue(options);


var app = express();

var bodyParser = require('body-parser')

app.use( bodyParser.json({limit: '10mb',parameterLimit: 50000}) );       // to support JSON-encoded bodies
app.use(bodyParser.urlencoded(
{     
  extended: true,// to support URL-encoded bodies
  limit: '10mb',
  parameterLimit: 50000
})); 

//cors
app.use(cors());



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
	'jobDeath',
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


function now(){
	return moment().format('MMMM Do YYYY, h:mm:ss a');
}

function event_msg(msg,client,job){
	// console.log(msg,client,job);

	if(!_.isUndefined(job) && !_.isUndefined(job.id)){
		console.log(' > '+'['+now()+'] '+ msg + ( !_.isUndefined(job.id) ? ' [JOB:ID '+ job.id +']': ''));

	}
	else{

		// console.log(job);
		console.log(' > '+'['+now()+'] '+ msg );
	}
}



app.get('/' , function(req, res) {
	res.end({
		server:'KAZI',
		msg:'We are ON!'
	})
});

app.post('/listclient' , function(req, res) {


	var client={
		ip:ip,
		name:req.body.name,
		jobIds:req.body.jobIds
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


app.post('/killJob' , function(req, res) {


	if( _.has(req.body,'id')){
		var ttl=parseFloat(req.body.ttl) || 0;

		//call killJob
		queue.killJob(req.body.id,ttl,function(err,diesAT){

			console.log(diesAT)

			res.send(_.merge(req.body,{diesAt:diesAT}))
		
		});

	}

	


});


app.post('/requestjob' , function(req, res) {

	var client=req.body.client || req.body,
		jobIds=req.body.jobIds || [];

	//listClient
	queue.requestJob(client,function(err,job){
		// console.log(JSON.stringify(job))
		
		if(err){
			res.send({});			
		}
		else{
			res.send(job);
		}
		
	},jobIds);


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
	To View Jobs. Only implemented Firebase....

*/


app.use('/firebase',express.static(path.join(__dirname, 'www','firebase'),{}));


app.get('/aggs' , function(req, res) {
	//update job
	

	res.send(req.params)

});


//set port
app.set('port', (config.host.port ||  process.env.PORT ||  2016) );


/*
	start listening...
*/
app.listen(app.get('port'), function(err) {

	if (err) return cb(err);

    // Find out which user used sudo through the environment variable
    var uid = parseInt(process.env.SUDO_UID);
    // console.log(uid);

     // Set our server's uid to that user
    if (uid){
    	process.setuid(uid);
    	console.log('Server\'s UID is now ' + uid );
    }

  	console.log('Express server listening on port ' + app.get('port'));
});

