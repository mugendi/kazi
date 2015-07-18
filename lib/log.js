var _ = require('lodash');
var log_client={};
var moment= require('moment');
// var log_client_name={};

var logger=function(server,logOpts,timezone){

	log_opts=(_.isArray(logOpts) && logOpts.length)?logOpts:[]

	process.env.TZ =timezone

	logOpts.forEach(function(log_opts){
		
	
		if(log_opts.client=='elasticsearch'){
			var logger_client = require(log_opts.client);
			log_client['elasticsearch'] = new logger_client.Client(log_opts.options || {} );
			// log_client_name='elasticsearch';
		}
		else if(log_opts.client=='firebase'){	
			var logger_client = require(log_opts.client);	
			log_client['firebase']= new logger_client(log_opts.options.data_url);
			// log_client_name='firebase'
		}

		if(_.size(server)){
			//now lets log
			logger.log(server,log_opts);	
		}		

	});
	
}


function formatJSON(JSON_Obj){

	if(_.isString(JSON_Obj)){
		try{
			JSON.parse(JSON_Obj);
		}
		catch(err){
			JSON_Obj={};
		}
	}


	for(var i in JSON_Obj){
		//if object

		if(_.isObject(JSON_Obj[i])){
			JSON_Obj[i]=formatJSON(JSON_Obj[i]);
		}
		else{
			JSON_Obj[i]=(/^[0-9]+$/.test(JSON_Obj[i]))?parseFloat(JSON_Obj[i]):JSON_Obj[i];
		}
	}


	return JSON_Obj;
}

logger.save={

	elasticsearch:function(obj,log_opts,evt){

		//log in ES;
		var index=log_opts.options.log.index || 'kazi';

		//add time like marvel does
		index = (index.indexOf('.')!==0)?'.'+index:index;

		// index+='-'+moment().format('YYYY.MM.DD')

		var formattedObj=formatJSON(obj);


		if(evt){
			log_client['elasticsearch'].create({
			  index: index,
			  type: evt,
			  // id: '1',
			  body: formattedObj
			}, function (error, response) {

			  // console.log(JSON.stringify(response,0,4))

			});	
		}		

	},
	firebase:function(obj,log_opts){
		//limit data objects for firebase
		log_client['firebase'].set(obj)
	}
}



logger.log=function(server,log_opts){

	
	var log_events=(_.isArray(log_opts.log_events))? log_opts.log_events :
		[ 'jobUpdated',  'jobQueued', 'jobFinished', 'jobsCleared', 'jobType', 'jobRescheduled', 'clientServed', 'clientNotServed', 'clientJobRequest', 'clientListed', 'clientFreed',  'clientBusy', 'clientExists' ];

	var job_log_name_pat=(log_opts.log_job_ids && _.isArray(log_opts.log_job_ids))?
			new RegExp(log_opts.log_job_ids.join('|')):/job.*/;

	log_events.forEach(function(evt){

		server.on(evt, function(msg,client,job){

			if(/^job.*/.test(evt) && job){
				//if a job is not to be logged, skip it
				if(!job_log_name_pat.test(job.name)){
					return 
				}
			}
			
			//new date
			var d=new Date();


			// _.pick(obj,_.first(_.keys(obj),20))


			if(client){
				var obj={
					client:client,
					job:_.merge(
						_.omit(job,'data'),
						
						{
							data: (job.data)? _.pick(job.data,_.first(_.keys(job.data),20)) : {}
						}
					) || {},
					msg:msg
				}	

				
				if(_.isFunction(logger.save[log_opts.client])){
					// console.log(log_opts.client)
					logger.save[log_opts.client](obj,log_opts,evt)
				}

			}
			

		});


	});	
	

	
}


module.exports = logger;


logger.prototype.msg_log=function(obj){

	if(obj && !_.isUndefined(logger.save['firebase'])){
		logger.save['firebase'](obj);
	}

}



module.exports = logger;
