var moment= require('moment');
var path = require('path');
var chokidar = require('chokidar');
var request = require('request');
var	_ = require('lodash');
var clearRequire = require('clear-require');


var Chance = require('chance');
var chance = new Chance();

var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
// numCPUs=1;
// console.log(numCPUs);

var port=2016;
var client={};

//jobs
var jobIds=[]

//Load workers..
var workers={};

//domains
var domain = require('domain');
var d = domain.create();

//config 
var config=require('../data/client-config.json');
var KAZI_server= config.server || 'http://localhost:'+port;

if(_.size(config.loggers)){

	var logger=require('./log.js')

	logger = new logger({},config.loggers,process.env.TZ);



}


var util=require('util')

/*if Domain Error*/
d.on('error', function(error) {

	// console.error('Error', error.stack);

	if(error.stack){
		logger.msg_log({
			error: error.stack
		});	
	}
	

	//kill this client
	var killtimer = setTimeout(function() {
      process.exit(1);
    }, 30000);

});

//Log Client Messages
console.log = function() { 

	var msgs=[];
	//create new logs each day
	for(var i in arguments){
		var d= arguments[i];

		process.nextTick(function(){
			process.stdout.write(util.format(d) + '\n');
			// msgs.push(util.format(d))

			logger.msg_log({
				log: util.format(d) 
			});

		});
			
	}

};

//now
function now(){
	return moment().format('MMMM Do YYYY, h:mm:ss a');
}

//loadWorkers
function loadWorkers(worker_path,job_ids,callback){

	//reload workers on change
	var file={},
		file_path='';

	var watcher = chokidar.watch(worker_path, {
		  //ignore dotfiles, ignore (node_modules|src|lib.*|data|config|dist|code)/*, ignore any other file other than .js
		  ignored: /[\/\\]\.|(node_modules|src|lib.*|data|config|dist|code)[\/\\].*|\.(json|txt|sql|html?)/,
		  persistent: true
		});

	watcher
	  .on('add', function(file_path) {
	  	//add file
	  	requireFile(file_path,worker_path);
	  })
	  .on('change', function(file_path) { 
	  	requireFile(file_path,worker_path); 
	  	job_ids=filter_job_ids(job_ids);
	  })
	  //callback when ready to start cluster
	  .on('ready', function(file_path) { 

	  	 job_ids=filter_job_ids(job_ids);

	  	 callback(workers,job_ids)
	  	 //filter job ids
  		
	  });
	
}

// requireFile: 
function requireFile(file_path,worker_path){

	//get the folder containng file...  		
	var _path=path.dirname(file_path);
	// console.log(_path,worker_path,file_path);
	_path=(_path==worker_path)? path.basename(file_path).split('.').shift() : path.basename(_path);


	//require file
	file=require(file_path);
	//clear from require cache
	clearRequire(file_path);

  	if(_.has(file,'run')){
  		console.log('['+now()+'] client Loading worker '+_path);	
  		workers[_path]={run:file.run}
  	}
  	//else remove worker if run function no longer exists
  	else if(_.has(workers,_path)){
  		console.log('['+now()+'] client Removing worker '+_path)
  		delete workers[_path]
  	}

}

// filter_job_ids
function filter_job_ids(job_ids){
	

	//stringify all worker keys(names)
	var worker_keys=_.keys(workers).join(','),
		pat='';

	// console.log(worker_keys)
	//loop thru all jobs
	for(var j in job_ids){
		pat=new RegExp('\\b'+job_ids[j]+'\\b')
		// console.log(pat)
		//if jobId does not match any existing jobs...delete id...
		if(! pat.test(worker_keys)){
			delete job_ids[j];
		}
	}

	//compact array
	return _.values(_.compact(job_ids));
}

/*
	Ask for Job
*/
function requestJob(){

	var post={
		url:KAZI_server+'/requestJob', 
		form: {
				client:client,
				jobIds:jobIds
			}
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
					// process.nextTick(requestJob);
					requestJob();
					// console.log(client.name)

				},20000);
			}
		}		
	});


}


// runJob
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
		if(_.has(workers,job.name)){

			// Run under domains
			d.run(function() {	


				try{

					workers[job.name].run(job,function(result){
						//clear tiumeout set to automatically terminateJobAfter x milliseconds
						clearTimeout(thisTimeout);
						//finish job
						finishJob(job,result);
					});	

				}
				catch (er2) {
					// oh well, not much we can do at this point.
					console.error('Error', er2.stack);

					//OK reschedule job immediately but with a delay
					var res=_.clone(job);
					res.delay=60;

					//finish job
					finishJob(job,res);

					//kill this client
					var killtimer = setTimeout(function() {
			          process.exit(1);
			        }, 30000);

			        // But don't keep the process open just for that!
			        killtimer.unref();

				}
			})
			
		}
		else{
			//clear tiumeout set to automatically terminateJobAfter x milliseconds
			clearTimeout(thisTimeout);
			//reschedule job immediately
			finishJob(job,job);
		}
		

	}
}

// finishJob
function finishJob(job,result){

	var post={
		url: KAZI_server +'/'+(_.isEmpty(result)?'finishJob':'updateJob'), 
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


// load workers
module.exports.start = function(workers_path,job_ids){
	//set jobIds to empty array by default
	

	if(_.isString(workers_path) ){

		//Now that all workers are loaded, start cluster processes...

		//load workers
		loadWorkers(workers_path,job_ids,function(workers,job_Ids){

			if(job_Ids.length==0 && job_ids.length){
				jobIds=['___^.^___'];
			}
			else{
				jobIds=job_Ids
			}


			/*//manage multiple clients via cluster...*/
			
			if (cluster.isMaster) {
			  // Fork workers.
			  for (var i = 0; i < numCPUs; i++) {
			    cluster.fork();
			  }

			  //restart workers who die
			  cluster.on('exit', function(worker, code, signal) {
			    
			    console.log( '['+now()+'] Oops! Worker running in PID : '+ worker.process.pid + ' died!');

			    cluster.fork();

			  });

			} else {
				//pick new name for worker
				client.name=chance.first();
				client.jobIds=_.values(jobIds);

				console.log('['+now()+'] '+"Hi. I'm "+ client.name + " and I'll be running with PID : "+cluster.worker.process.pid);

				var post={
					url:KAZI_server +'/listclient', 
					form: client
				}

				console.log('['+now()+'] '+client.name +': Finished job...');

				// console.log(post);
				request.post(post, function(err,httpResponse,body){
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

		});
		
	}
	else{
		console.error(new Error( "'workers_path' must be a string"));
	}

} 
