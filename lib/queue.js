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
	clientsKey='KAZI:clients',
	busyClientsHashKey='KAZI:clients:busy';

//job priorities
var jobPriorities='high,normal,low'.split(',');
var jobState={'active':-1,'finished':-2};
var autoclearFinishedJobsAfter=2*24*3600 ;//2 days;
var rescheduleStuckJobsAfter=60*5; //5 hours (always in seconds)
var autoclearFinishedJobs=true;


// create the class
var server={},
	Queue = function (options) {
		var opts={
			maxClients:100,
			timeoutLazyClientsFor:60*1000,
			expireClientsAfter:(3600),
			strictFIFO:false,
			autoclearFinishedJobs:autoclearFinishedJobs,
			rescheduleStuckJobsAfter:rescheduleStuckJobsAfter
		};

		server=this;

		server.options=_.extend(opts,options);

		autoclearFinishedJobs=(server.options.autoclearFinishedJobs);
		rescheduleStuckJobsAfter=parseInt(server.options.rescheduleStuckJobsAfter) || rescheduleStuckJobsAfter;
		
	};

// augment the prototype using util.inherits
util.inherits(Queue, EventEmitter);



//subscribe to all jobs
subscriber.psubscribe(jobSetKey+':*:*'); 

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
		
		//set numeric priority value
		var priority=0;
		var delay=0;

		// to ensure strictFIFO, job priorities are set to the time they are scheduled

		if(server.options.strictFIFO || !_.isUndefined(job.delay) ){
			//for strict FIFO, schedule jobs i seconds form now where i is the job index in an array
			job.priority=moment().add(i,'seconds').unix();

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


		//
		if(_.isNumber(job.priority)){
			priority=job.priority;
		}
		else if(_.isString(job.priority)){
			priority=_.indexOf(jobPriorities,job.priority);
		}
		
		//no negative priorities...
		if(priority<0){priority=1;}

		// console.log(priority)

		server.uniqueJob(job,function(err,unique){
			// console.log(priority,job)
			if(unique || update){

				//create job with channel, then call server to 
				redisClient.hset(jobHashKey,job.id,JSON.stringify(job),function(err,res){
					//list job in ordered set to help by-priority ordering
					redisClient.zadd(jobOrderKey,priority,job.id);
					
					//emit event
					var evt=update?'Updated':'Queued';

					server.emit('job'+evt,'Job '+(evt.toLowerCase())+( (client) ? ' by Client ('+client.name+')':'')+ (delay?'. A delay of '+delay+' seconds has also been effected!':'') + '.', job);

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
				server.emit('jobFinished','Job finished by Client ('+client.name+').', job);
			});

		});	

	});

	
}
/*
	update job requests
*/
Queue.prototype.updateJob = function(client,job,result,callback){
	callback=callback || function(){};

	// console.log(result);

	if(_.isUndefined(result.id)){
		callback(new Error('Job must have a key!'),null)
	}
	else{

		var stateTime=moment().unix();


		//mark job as finished
		redisClient.zadd(jobCompleteSetKey,stateTime,JSON.stringify(job),function(err,res){

			//update job by requeuing it
			server.queueJob(result,true,function(job){
				//ok lets callback to allow client continue with other requests
				
				//free client 
				server.freeClient(client,function(err,client){
					callback(null,true);
				});


			},client);

		});
	}
	
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
Queue.prototype.requestJob = function(client,callback){
	callback=callback || function(){};

	server.emit('clientJobRequest','Client ('+client.name+') has requested for a new job.', client);

	// console.log(client)
	if(_.isUndefined(client.name)){
		callback(new Error('Rejected'),{})
	}
	else{

		//tomorrow
		var now=moment().unix();

		//get job id with highest priority from ordered set
		redisClient.zrangebyscore(jobOrderKey,0,now,'limit',0,1,function(err,job_ids){

			// console.log(err,job_ids)

			if(job_ids.length){

				//see if client is busy
				server.isBusyClient(client,function(err,isBusy){

					// if(isBusy){
					// 	//client busy!!! Why ask for more jobs!
					// 	//emit 
					// 	server.emit('clientBusy','Client ('+client.name+') is busy! Cant serve new job!', client);
					// 	callback(new Error('Lazy Client!'),{});
					// }
					// else{
						
						//use job id to fetch job from hash
						redisClient.hget(jobHashKey,job_ids,function(err,res){
							var job=JSON.parse(res);

							if(job){
								//list client as one requesting a job
								redisClient.hset(busyClientsHashKey,client.name,job.id,function(err,res){

									//change job score to -1 to show active
									redisClient.zadd(jobOrderKey,(now*-1),job.id,function(err,res){
										//callback with job
										callback(null,job);
										//emit job served
										server.emit('clientServed','Client ('+client.name+') served.', client, job);
									});
									
								});	
							}
							else{
								callback(new Error('No Job!'),{});
								server.emit('clientNotServed','Client ('+client.name+') NOT served! There are no jobs to serve!', client, {});

								return;

							}

						});	
						
					// }

				});				

			}
			else{
				callback(new Error('No Job!'),{});
				server.emit('clientNotServed','Client ('+client.name+') NOT served! There are no jobs to serve!', client, {});
			}
			
		});

	}
}


/*
	to check if client is busy
*/
Queue.prototype.isBusyClient = function(client,callback){
	callback=callback || function(){};

	redisClient.hget(busyClientsHashKey,client.name,function(err,res){
		callback(null,((res && res!==null)?true:false));		
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
							number:res+1,
							name:client.name
						};	

				//list client
				redisClient.set(clientKey, JSON.stringify(clientObj) ,function(err,res){

					//expire client
					server.expireClient(clientObj,function(err,client){

						callback(null,clientObj);
						//emit client liste
						server.emit('clientListed','Client ('+client.name+') listed and expires in '+server.options.expireClientsAfter+' seconds.', clientObj);	

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
			var time_to_expire=((moment().unix())-autoclearFinishedJobsAfter), // (autoclearFinishedJobsAfter) microseconds ago
				start_of_time=(moment([1970, 1, 01]).unix())//start of time
				setCmd=[jobCompleteSetKey,start_of_time,time_to_expire]; //delete any keys between 1970 and (autoclearFinishedJobsAfter) microseconds ago

			//remove old complete jobs
			redisClient.zremrangebyscore(setCmd,function(err,res){
				if(res){
					//emit job cleared
					server.emit('jobsCleared',res+' finished Jobs older than '+(autoclearFinishedJobs/1000)+' seconds successfully cleared!');
				}
	 		
	 		});		
		}
		

		if(rescheduleStuckJobsAfter){
			var stuck_since=(moment().unix()-rescheduleStuckJobsAfter)*-1;

			setCmd=[jobOrderKey,stuck_since,-1, 'limit',0,5];

			redisClient.zrangebyscore(setCmd,function(err,job_ids){
				// console.log(err,job_ids);

				job_ids.forEach(function(job_id){

					//reschedule job from the negative scale to the positive we use known job.priority 
					redisClient.hget(jobHashKey,job_id,function(err,res){
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

