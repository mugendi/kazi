var _ = require('lodash'),
	jobSetKey='KULA:jobs',
	jobHashKey='KULA:existing'
	clientsHashKey='KULA:clients'
	;

var request = require('request');

var jobs=[];
var jobTypes=[
		'twitter',
		'facebook',
		'categoright'
	];

//config 
var config=require('./data/config.json');
var KAZI_server= (!_.isUndefined(config.host) && (!_.isUndefined(config.host.url) && !_.isUndefined(config.host.port)))? config.host.url+':'+config.host.port : 'http://localhost:' + (config.host.port || 2016);

console.log(KAZI_server)

/*Categoright*/

jobs.push(
	{
		priority:'normal',
		id:'categoright:getTitles',
		name:'categoright',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{
			method:'getTitles'
		}
	}
);



/*twitter.tracking.update_users*/
jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.update_users',
		name:'twitter.tracking.update_users',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{
			method:'updateUsers'
		}
	}
);





/**/



/*twitter.tracking.trending*/
jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.trending',
		name:'twitter.tracking.trending',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{
			locale:'ke',
			woeid:1528488 //2345940
		}
	}
);

// 


/*twitter.tracking.engagement*/
jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.engagement',
		name:'twitter.tracking.engagement',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{}
	}
);



jobs.push(
	{				
		name:'twitter.tracking.track',
		id:'twitter:1:ecitizenke',
		
		job_id:'2', //should be an interger but can be postfixed with '.Something.AnotherSomething'
		_index:'tracking',
		_type: 'twitter',
		end_point:'streaming',
		terms:['#ecitizenke'],

		data:{			
			max_id:0,
			since_id:0,						
		}
	}
)



/*twitter.tracking.shorturls*/
jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.shorturls',
		name:'twitter.tracking.shorturls',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{}
	}
);



// /*twitter.tracking.track_trend*/
// jobs.push(
// 	{
// 		priority:'normal',
// 		id:'twitter.tracking.track_trend',
// 		name:'twitter.tracking.track_trend',
// 		terminateJobAfter: (10*1000*60), //10 mins
// 		delay:0,
// 		data:{
// 			locale:'ke'
// 		}
// 	}
// );


// jobs=[]

jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.update_empty_engagement',
		name:'twitter.tracking.update_empty_engagement',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{}
	}
);



/*twitter.tracking.engagement*/
jobs.push(
	{
		priority:'normal',
		id:'twitter.tracking.engagement',
		name:'twitter.tracking.engagement',
		terminateJobAfter: (10*1000*60), //10 mins
		delay:0,
		data:{}
	}
);





var post={
		url:KAZI_server+'/queueJob', 
		form: jobs
}


console.log(JSON.stringify(post,0,4));


// //first register client
request.post(post, function(err,httpResponse,body){ 
	if(body){
		console.log(body)
	}
			
});

