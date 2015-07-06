var _ = require('lodash');
var log_client={};

var logger=function(server,log_opts,timezone){
	var logger_client = require(log_opts.client);

	process.env.TZ =timezone

	if(log_opts.client=='elasticsearch'){
		log_client = new logger_client.Client(log_opts.options || {} );
	}
	else if(log_opts.client=='firebase'){		
		log_client= new logger_client(log_opts.options.data_url);
	}

	//now lets log
	logger.log(server,log_opts);
}



logger.log=function(server,log_opts){
	

	var log_events=(_.isArray(log_opts.log_events))?log_opts.log_events :
		[ 'jobUpdated',  'jobQueued', 'jobFinished', 'jobsCleared', 'jobType', 'jobRescheduled', 'clientServed', 'clientNotServed', 'clientJobRequest', 'clientListed', 'clientFreed',  'clientBusy', 'clientExists' ];

	log_events.forEach(function(evt){

		server.on(evt, function(msg,client,job){

			// console.log(client)

			var d=new Date();


			if(client){
				var obj={
					client:client,
					job:_.merge(
						_.pick(job,'id','name','priority','terminateJobAfter','delay'),
						
						{
							data:_.omit(job,'id','name','priority','terminateJobAfter','delay')
						}
					) || {},
					msg:msg
				}	

				
				if(_.isFunction(logger.save[log_opts.client])){
					logger.save[log_opts.client](obj)
				}

			}
			

		});


	});
}


logger.save={

	elasticsearch:function(obj){
		//log in ES;
		var index=server.options.log.index || 'kazi';
		// console.log(index,evt)

		log_client.create({
		  index: index,
		  type: evt,
		  // id: '1',
		  body: obj
		}, function (error, response) {
		  // console.log(JSON.stringify(response,0,4))
		});

	},
	firebase:function(obj){
		// console.log(obj)
		log_client.set(obj)
	}
}

module.exports = logger;

//load log client
		if(2==1){

			
			

			
		}