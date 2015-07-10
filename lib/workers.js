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
numCPUs=1;
// console.log(numCPUs);

var port=2016;
var client={};

//jobs
var jobIds=[]

//Load workers..
var workers={};

//domain
var Domain = require('domain');
var domain = Domain.create();

var util=require('util');


//config 
var config=require('../data/client-config.json');
var KAZI_server= config.server || 'http://localhost:'+port;

if(_.size(config.loggers)){

	var logger=require( './log.js' );

	logger = new logger({},config.loggers,process.env.TZ);

}



/*if Domain Error*/
domain.on('error', function(error) {

	console.log(error.stack)

	domain.rescue(domain,function(res){
		/*Ok, we have handled our escape. Let's kill this client now*/
		process.exit(1);
	})

	

	/*
		Let's log this errors to the front end...
	*/
	logger.msg_log({
		error: util.format(error.stack)
	});
	

	/*
		If we wait for some 60 seconds, we should expect user has handled errors via escape, 
		Too bad if they havent done so coz at this point, we must exit at all cost!
	*/
	var killtimer = setTimeout(function() {
      process.exit(1);
    }, 60000);

});

/*Use Domains to pass & Log client Errors & Messages*/
domain.log = function() { 

	//create new logs each day
	for(var i in arguments){

		console.log(arguments[i]);

		//log msg			
		logger.msg_log({
			log: util.format(arguments[i]) 
		});			
	}

};

/*Use Domains to pass & Log client Errors & Messages*/
domain.error = function() { 

	//create new logs each day
	for(var i in arguments){

		console.log(arguments[i]);

		//log msg			
		logger.msg_log({
			error: util.format(arguments[i]) 
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
		  ignored: /[\/\\]\.|(node_modules|src|lib.*|data|config|dist|code)[\/\\].*|\.(json|txt|sql|log|html?)/,
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

	  	 // console.log(workers)
	  	 //filter job ids
  		
	  });
	
}

// requireFile: 
function requireFile(file_path,worker_path){

	//get the folder containng file...  		
	var _path=path.dirname(file_path);

	// console.log(_path,worker_path,file_path);
	_path=(_path==worker_path)? path.basename(file_path).split('.').shift() : 
								file_path.replace(worker_path,'') //remove worker_path
									 .replace(/^[\\\/]|[\\\/]$/,'') //remove start or trailing slashes
									 .replace(/[\\\/]/g,'.') //replace slashes with dots
									 .replace(/\.index\.js$/,'')//replace index.js
									 .replace(/\.js$/,'') //replace .js
									 ; 

	try{
		//require file
		file=require(file_path);
		//clear from require cache
		clearRequire(file_path);

	  	if(_.has(file,'run')){
	  		console.log('['+now()+'] client Loading worker '+_path);	
	  		workers[_path]={
	  			run: file.run,
	  			rescue: file.rescue || function(domain,callback){
	  				//finish this job
			        domain.finishJob(domain.job,domain.job,function(){
			          //callback to kill this process
			          callback();
			        });
	  			}
	  		}
	  	}
	  	//else remove worker if run function no longer exists
	  	else if(_.has(workers,_path)){
	  		console.log('['+now()+'] client Removing worker '+_path)
	  		delete workers[_path]
	  	}
	}
	catch(err){
		domain.error(err)
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

		domain.log('['+now()+'] '+client.name +': Running job...');

		//automatically terminate job if not finished in time
		var terminateJobAfter=job.terminateJobAfter || (5*60*1000); //5 minutes

		//set timeout after which we must forcefully terminate a job
		var thisTimeout=setTimeout(function(){
			domain.log( '['+now()+'] '+client.name +': Forcefully Teminating [JOB:'+job.id+']' );
			//forcefully terminate job
			finishJob(job,result);

		},terminateJobAfter);

		//every job should have an identifier (name/id/worker) that determines how the job is to be handled
		// console.log(job);

		//run job....
		//pass result object (another job to reschedule) to finishJob
		if( _.has(workers,job.name) && _.has(workers[job.name],'run') ){

			domain.job=job;
			domain.callback=callback;
			domain.rescue=workers[job.name].rescue ;
			domain.finishJob=finishJob;


			// Run under domain
			domain.run(function() {	

				workers[job.name].run(domain,function(result){
					//clear tiumeout set to automatically terminateJobAfter x milliseconds
					clearTimeout(thisTimeout);
					//finish job
					finishJob(job,result,function(){
				
					});
				});	

			})
			
		}
		else{

			domain.error('Worker '+ job.name +' doesn\'t exist or lacks the run() method')

			//clear tiumeout set to automatically terminateJobAfter x milliseconds
			clearTimeout(thisTimeout);
			//reschedule job immediately
			finishJob(job,job,function(){

			});
		}
		

	}
}

// finishJob
function finishJob(job,result,callback){

	callback=callback || function(res){};

	var post={
		url: KAZI_server +'/'+(_.isEmpty(result)?'finishJob':'updateJob'), 
		form: {client:client,job:job,result:result}
	}

	domain.log('['+now()+'] '+client.name +': Finished job...');

	// console.log(post);

	request.post(post, function(err,httpResponse,body){
		// console.log(body);

		callback();

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
