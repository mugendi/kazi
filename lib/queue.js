var redis = require("redis"),
    subscriber = redis.createClient(),
    redisClient = redis.createClient(),
    publisher = redis.createClient(),

    EventEmitter = require('events').EventEmitter,
	util = require('util'),
	_ = require('lodash'),
	moment = require('moment')

	jobSetKey='KAZI:jobs',
	jobCompleteSetKey='KAZI:jobs:complete',
	jobOrderKey='KAZI:jobs:order',
	jobHashKey='KAZI:jobs',
	jobDeathOrder='KAZI:jobs:deathOrder'
	clientsKey='KAZI:clients',
	busyClientsHashKey='KAZI:clients:busy';


//job priorities & other options
var jobPriorities='high,normal,low'.split(',');
var jobState={'active':-1,'finished':-2};
var autoclearFinishedJobsAfter=2*24*3600 ;//2 days;
var rescheduleStuckJobsAfter=60*5; //5 mins (always in seconds)
var autoclearFinishedJobs=true;
var logger=require('./log.js')

var config=require('../data/config.json');

//set timezone
process.env.TZ =config.timezone ||  'Africa/Nairobi' 


// create the class
var server={},
	Queue = function (options) {
		var opts={
			maxClients:100,
			timeoutLazyClientsFor:60*1000,
			expireClientsAfter:(3600),
			strictFIFO:false,
			autoclearFinishedJobs:autoclearFinishedJobs,
			rescheduleStuckJobsAfter:rescheduleStuckJobsAfter,
			runMultipleJobs:false,
			logger:{}
		};

		server=this;

		server.options=_.extend(opts,config,options);

		autoclearFinishedJobs=(server.options.autoclearFinishedJobs);
		rescheduleStuckJobsAfter=parseInt(server.options.rescheduleStuckJobsAfter) || rescheduleStuckJobsAfter;

		
		if(_.size(server.options.loggers)){
			// console.log(logger)
			var log = new logger(server,server.options.loggers,process.env.TZ);
		}


		//clear all clients previously registered
		redisClient.keys(clientsKey+'*',function(error,keys){

			if(!error){
				keys.forEach(function(key){
					redisClient.del(key)
				})
			}
			
		});
		
	};

// augment the prototype using util.inherits
util.inherits(Queue, EventEmitter);



/*
	To queue jobs in server
*/
Queue.prototype.queueJob= function (jobs,update,callback,client){
	callback=callback || function(){};

	if(!_.isArray(jobs) && !_.isObject(jobs)){
		//wrong job
		//emit
		server.emit('jobType','Wrong job Type. Jobs can only be submitted as arrays or objects.', job);
	}

	if(!_.isArray(jobs)){ jobs=[jobs]; }

	jobs.forEach(function(job,i){
		
		//if Unique JOB
		server.uniqueJob(job,function(err,unique){


			if(unique || update){

				//set numeric priority value
				var priority=0;
				var delay=0;
				var ttl=0;


				// to ensure strictFIFO, job priorities are set to the time they are scheduled

				if(server.options.strictFIFO || !_.isUndefined(job.delay) ){
					//for strict FIFO, schedule jobs i seconds form now where i is the job index in an array

					//if job priority is set to high, shedule NOW by setting priority to the lowest possible value, 0
					job.priority=( typeof job.priority!=='undefined' && job.priority.toLowerCase()=='high') ? 0 : moment().add(i,'seconds').unix();

					if(!_.isUndefined(job.delay)){
						job.delay=parseInt(job.delay) || 0;
						delay=job.delay;

						job.priority=moment().add(job.delay, 'seconds').unix();

						//ok no more delays
						delete job.delay;	
					}
					
				}
				else{
					job.priority=(parseFloat(job.priority)) || job.priority || 1;
				}


				//Enter Job status, we always start with idle
				// job.jobStatus='idle'

				//
				if(_.isNumber(job.priority)){
					priority=job.priority;
				}
				else if(_.isString(job.priority)){
					priority=_.indexOf(jobPriorities,job.priority);
				}

				if(!_.isUndefined(job.ttl)){
					ttl=parseFloat(job.ttl);

					delete job.ttl;
				}

				//add job version
				job.version=(job.version)?parseInt(job.version)+1 : 1;
				job.served=(job.served)?job.served:0;

				//add scheduled time (scheduled,updated,timeAlive,roundTime )
				var nowUnix= moment().unix()*1000,
					now=moment(),
					served=(job.served)?moment(job.served, moment.ISO_8601):0,
					scheduled=moment(job.scheduled, moment.ISO_8601);

				
				job.roundTime=(served)?now.diff(served) || 0 : 0;
				job.timeAlive=(job.scheduled)? now.diff(scheduled):0;
					
				job.updated= now.toISOString();
				job.scheduled= job.scheduled || now.toISOString();

				// console.log(JSON.stringify(job,0,4))

				//always have a data field
				job.data= job.data || {};

				// moment(String, moment.ISO_8601);				

				//no negative priorities...
				if(priority<0){priority=1;}

				//create job with channel, then call server to 
				redisClient.hset(jobHashKey,job.id,JSON.stringify(job),function(err,res){

					//list job in ordered set to help by-priority ordering
					redisClient.zadd(jobOrderKey,priority,job.id,function(err,res){

						//if we will need to kill this job at a future time
						if(ttl>0){

							//enter job in the death list
							server.killJob(job.id,ttl,function(err,dieAt){
								//remove ttl
								server.emit('jobDeath','Job [JOB:ID '+job.id+'] will die at '+dieAt+"",job);
							})
						}
	

					});

					
				
					//emit event
					var evt=update?'Updated':'Queued';

					server.emit('job'+evt,'Job '+(evt.toLowerCase())+( (client) ? ' by Client ('+client.name+')':'')+ (delay?'. A delay of '+delay+' seconds has also been effected!':'') + '.', client, job);

				});

			}
		});	
	});


	callback(null,jobs);
}




/*
	function to ensure unique jobs
*/
Queue.prototype.uniqueJob= function (job,callback){
	callback=callback || function(){};

	if(!_.isObject(job)){
		job={id:job};
	}

	//job must have ID
	if(_.isUndefined(job.id)){
		callback(new Error('Job must have a key!'),null)
	}
	else{
		//check if job is unique
		redisClient.hget(jobHashKey,job.id,function(err,res){
			callback (null,(!res)) ;
		});
	}
}

/*
	detete job requests
*/
Queue.prototype.finishJob = function(client,job,callback){
	callback=callback || function(){};


	var stateTime=moment().unix();


	//mark job as finished
	redisClient.zadd(jobCompleteSetKey,stateTime,JSON.stringify(job),function(err,res){


		//set remove job from job order
		redisClient.zrem(jobOrderKey,job.id,function(err,res){

			//remove job from jobHashKey
			redisClient.hdel(jobHashKey,job.id,function(err,res){

				//free this client
				server.freeClient(client,function(err,client){
					callback(null,true);
				});
				
				//emit job completion
				server.emit('jobFinished','Job finished by Client ('+client.name+').',client, job);
			});

		});	

	});	
}

/*
	update job requests
*/
Queue.prototype.updateJob = function(client,job,result,callback){

	var stateTime=moment().unix();
	var now=moment().unix();


	//finish job
	server.finishJob(client,job,function(){
		//update job by requeuing it

		//check if job was scheduled for death
		redisClient.zrangebyscore(jobDeathOrder,0,now,function(err,jobIds){

			//if job id within matched ids, dont requeue job to ensure it dies
			if(_.indexOf(jobIds,job.id)>-1){
				//emit event
				server.emit('jobDeath','Job [JOB:ID '+job.id+'] has died after ttl (Time to live) has elapsed!',client, job);

				//remove job from list of death
				redisClient.zrem(jobDeathOrder,job.id);


				return callback(null,true);
			}
			else{

				//update job by requeuing it
				server.queueJob(result,true,function(job){
					//ok lets callback to allow client continue with other requests
					callback(null,true);

				},client);	
			}				

		})

	});
	
}


/*
	Manage JobDeathList to determine when & how jobs fdie
*/
Queue.prototype.killJob= function (job_id,ttl,callback){
	callback=callback || function(){};

	var TTL=(parseFloat(ttl) || 0 );

	//ensure job exists
	server.uniqueJob(job_id,function(err,unique){

		if(!unique){
			//calculate job death
			var dieAt=moment().add(TTL,'seconds').unix(),
				dieAtTime=moment().add(TTL,'seconds').format();


			//change job score to -1 to show active
			redisClient.zadd(jobDeathOrder,dieAt,job_id,function(err,res){

				server.emit('jobDeath','Job [JOB:ID '+job_id+'] has been scheduled to die at '+dieAtTime+' after TTL (Time to live) of '+TTL+' seconds!',{},{});

				// console.log(err,res)
				callback(null,dieAtTime);

			});		
		}
		else{
			callback(null,'-N/A-');
		}

	})

	

}


/*
	Free client 
*/
Queue.prototype.freeClient=function(client,callback){
	callback=callback || function(){};

	//delete client from busyClientsHashKey
	redisClient.hdel(busyClientsHashKey,client.name,function(err,res){
		//emit event
		server.emit('clientFreed','Client ('+client.name+') freed. Client can now request new jobs!', client);

		callback(null,client);
	});

}

/*
	Get job requests
*/
Queue.prototype.requestJob = function(client,callback,jobIds){
	jobIds=jobIds || [];

	callback=callback || function(){};

	server.emit('clientJobRequest','Client ('+client.name+') has requested for a new job.', client, {});

	// console.log(client)
	if(_.isUndefined(client.name)){
		callback(new Error('Rejected'),{})
	}
	else{

		// //if jobIDS

		var matchPat=/.*/;
		if(_.isArray(jobIds) && jobIds.length){
			matchPat=new RegExp(jobIds.join('|'),'ig');
		}


		//NOW
		var now=moment().unix();

		// console.log('zrangebyscore '+jobOrderKey+' '+0+' '+now)

		//get job id with highest priority from ordered set
		redisClient.zrangebyscore(jobOrderKey,0,now,function(err,jobIds){
			var jobId=null;

			// console.log(err,jobIds)

			if(jobIds.length){
				for(var id in jobIds){
					if(matchPat.test(jobIds[id])){
						jobId=[jobIds[id]];
						break;
					}
				}
			}

			if(jobId){

				// console.log(jobId)

				//see if client is busy
				server.isBusyClient(client,function(err,isBusy){

					if(isBusy){
						//client busy!!! Why ask for more jobs!
						//emit 
						server.emit('clientBusy','Client ('+client.name+') is busy! Cant serve new job!', client, {});
						callback(new Error('Lazy Client!'),{});
					}
					else{
						
						server.pickJob(client, jobHashKey, jobId, callback);

					}

				});				

			}
			else{
				callback(new Error('No Job!'),{});
				server.emit('clientNotServed','Client ('+client.name+') NOT served! There are no jobs to serve!', client, {});
			}
			
		});

	}
}


Queue.prototype.pickJob= function(client, jobHashKey,jobIds,callback){
	//NOW
	var now=moment().unix();
	
	// console.log('hgetall '+jobHashKey+' '+jobIds);

	redisClient.hget(jobHashKey,jobIds,function(err,res){

		var job=JSON.parse(res);

		//if we have job and job is not busy
		if(job /*&& job.jobStatus!=='busy'*/){

			//list client as one requesting a job
			redisClient.hset(busyClientsHashKey,client.name,job.id,function(err,res){

				//change job score to -1 to show active
				redisClient.zadd(jobOrderKey,(now*-1),job.id,function(err,res){
					
					//save job status to active
					/*var jobStatus= (server.options.runMultipleJobs===true) ? 'idle' : 'busy';
					job.jobStatus=jobStatus;
					//since when has this job been in busy state?
					job.busySince=now;*/


					redisClient.hset(jobHashKey,job.id,JSON.stringify(job),function(err,res){
						
						//add job served time 
						job.served=moment().toISOString();

						//callback with job
						callback(null,job);

						//emit job served
						server.emit('clientServed','Client ('+client.name+') served.', client, job);

						return;
					});
					
				});
				
			});	

		}
		else{
			callback(new Error('No Job!'),{});
			server.emit('clientNotServed','Client ('+client.name+') NOT served! There are no jobs to serve!', client, {});

			//if job has been in busy state for too long...

			return;
		}

	});	

}



/*
	to check if client is busy
*/
Queue.prototype.isBusyClient = function(client,callback){
	callback=callback || function(){};

	redisClient.hget(busyClientsHashKey,client.name,function(err,res){
		// callback(null,((res && res!==null)?true:false));
		//until we fix busy client management, return false
		callback(null,false);		
	});

};


/*
	set update client expiry
*/
Queue.prototype.expireClient = function(client,callback){
	callback=callback || function(){};

	if(_.isUndefined(client.name)){
		callback(new Error('Rejected'),null)
	}
	else{

		clientKey=clientsKey+':'+client.name;

		//set/update client expiry
		redisClient.expire(clientKey,server.options.expireClientsAfter,function(err,res){
			// console.log(err,res);
			callback(null,client);
		});

	}
}

/*
	to list clients
*/
Queue.prototype.listClient = function(client,callback){
	callback=callback || function(){};

	if(_.isUndefined(client.name)){
		callback(new Error('Rejected'),null)
	}
	else{

		var clientKey=clientsKey+':'+client.name;

		//find listed clients
		redisClient.get(clientKey,function(err,res){

			if(!res){
				var clientObj={
							ip:client.ip,
							time:moment().format(),
							name:client.name,
							jobIds:client.jobIds
						};	

				//list client
				redisClient.set(clientKey, JSON.stringify(clientObj) ,function(err,res){

					//expire client
					server.expireClient(clientObj,function(err,client){

						callback(null,clientObj);
						//emit client liste
						server.emit('clientListed','Client ('+client.name+') listed and expires in '+server.options.expireClientsAfter+' seconds.', clientObj,{});	

					});					

				});


				//progress client expiry
				server.expireClient(client,function(err,client){

					//automatically free this client after server.options.timeoutLazyClientsFor has elapsed
					setTimeout(
						function () {
						// body...
						server.freeClient(client);

					},server.options.timeoutLazyClientsFor);	

				});

			}
			else{
				var clientObj=JSON.parse(res);

				// callback
				callback(null,clientObj);

				//emit client liste
				server.emit('clientExists','Client ('+client.name+') already listed!', clientObj);
			}
			
		});	
	}
}



//if autoclear jobs
if(autoclearFinishedJobs){

	setInterval(function(){

		if(autoclearFinishedJobs){

			console.log(autoclearFinishedJobsAfter)
			var time_to_expire=(moment().subtract(autoclearFinishedJobsAfter,'seconds').unix()), // (autoclearFinishedJobsAfter) microseconds ago
				start_of_time=(moment([1970, 1, 01]).unix())//start of time
				setCmd=[jobCompleteSetKey,start_of_time,time_to_expire]; //delete any keys between 1970 and (autoclearFinishedJobsAfter) seconds ago

			console.log(setCmd, (moment().subtract(autoclearFinishedJobsAfter,'seconds').toISOString()))

			//remove old complete jobs
			redisClient.zremrangebyscore(setCmd,function(err,res){

				console.log(JSON.stringify(res,0,4))

				if(res){
					//emit job cleared
					server.emit('jobsCleared',res+' Clearing Jobs older than '+(autoclearFinishedJobsAfter)+' seconds successfully cleared!');
				}
	 		
	 		});		
		}
		

		if(rescheduleStuckJobsAfter){

			var stuck_since=(moment().subtract(rescheduleStuckJobsAfter,'seconds').unix())*-1;

			setCmd=[jobOrderKey,stuck_since,-1, 'limit',0,5];

			redisClient.zrangebyscore(setCmd,function(err,jobIds){
				// console.log(err,jobIds);

				jobIds.forEach(function(jobId){

					//reschedule job from the negative scale to the positive we use known job.priority 
					redisClient.hget(jobHashKey,jobId,function(err,res){
						if(res){
							var job=JSON.parse(res);

							// console.log(job.priority);

							//reorder job with known priority
							redisClient.zadd(jobOrderKey,job.priority,job.id);

							//job rescheduled (emit
							server.emit('jobRescheduled','Job ID:'+job.id+' has been successfully rescheduled after being in active for '+rescheduleStuckJobsAfter+' seconds', job);

						}

					});
				});

				
			});

		}

		


	},5000);



}






//
module.exports=Queue;

