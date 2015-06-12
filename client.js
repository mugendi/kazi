var request = require('request'),
	_ = require('lodash'),
	client={};

var Chance = require('chance');
var chance = new Chance();

var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
var port=2016;
var worker=require('./workers');
var moment=require('moment');

// numCPUs=1;

// console.log(numCPUs);

function now(){
	return moment().format('MMMM Do YYYY, h:mm:ss a');
}

/*
	Ask for Job
*/
function requestJob(){

	var post={
		url:'http://localhost:'+port+'/requestJob', 
		form: client
	}


	console.log('['+now()+'] '+client.name +': Requesting new job...');

	//first register client
	request.post(post, function(err,httpResponse,body){


		if(body){
			var job=JSON.parse(body);


			if(!_.isEmpty(job)){
				console.log('['+now()+'] '+client.name +': Job allocated [JOB:'+job.id+']');

				//crunch job
				runJob(job,function(job){
					// console.log(job);
				});	
			}
			else{
				console.log('['+now()+'] '+client.name +': ...waiting before next Job Request!');

				//take a break before asking for other job
				setTimeout(function(){
					process.nextTick(requestJob);
				},20000);
			}
		}		
	});


}

function runJob(job,callback){

	if(job && !_.isUndefined(job.name)){

		//by default, the result = job to ensure we reschedule job on forceful termination
		var result=job;

		console.log('['+now()+'] '+client.name +': Running job...');

		//automatically terminate job if not finished in time
		var terminateJobAfter=job.terminateJobAfter || (5*60*1000); //5 minutes

		//set timeout after which we must forcefully terminate a job
		var thisTimeout=setTimeout(function(){
			console.log( '['+now()+'] '+client.name +': Forcefully Teminating [JOB:'+job.id+']' );
			//forcefully terminate job
			finishJob(job,result);

		},terminateJobAfter);

		//every job should have an identifier (name/id/worker) that determines how the job is to be handled
		// console.log(job);

		//run job....
		//pass result object (another job to reschedule) to finishJob

		worker[job.name](job,function(result){
			//clear tiumeout set to automatically terminateJobAfter x milliseconds
			clearTimeout(thisTimeout);
			//finish job
			finishJob(job,result);
		});

	}
}

function finishJob(job,result){

	var post={
		url:'http://localhost:'+port+'/'+(_.isEmpty(result)?'finishJob':'updateJob'), 
		form: {client:client,job:job,result:result}
	}

	console.log('['+now()+'] '+client.name +': Finished job...');

	// console.log(post);

	request.post(post, function(err,httpResponse,body){
		// console.log(body);
		//ask for another job
		setTimeout(function(){
			process.nextTick(requestJob);
		},5000);		
	});

}



//manage multiple clients via cluster...
if (cluster.isMaster) {
  // Fork workers.
  for (var i = 0; i < numCPUs; i++) {
    cluster.fork();
  }

  //restart workers who die
  cluster.on('exit', function(worker, code, signal) {
    
    console.log( '['+now()+'] OOps! Worker running in PID : '+ worker.process.pid + ' died!');

    cluster.fork();

  });

} else {


	//pick new name for worker
	client.name=chance.first();

	console.log('['+now()+'] '+"Hi. I'm "+ client.name + " and I'll be running with PID : "+cluster.worker.process.pid);

	
	//first register client
	request('http://localhost:'+port+'/listclient?name='+ client.name , function (error, response, body) {

		var json=JSON.parse(body || '{}');

		//if properly formated
		if(typeof json.name !== 'undefined'){
			client=json;
			//Ok now let's start the hard work...
			// ask for jobs
			requestJob();
		}

	});

}