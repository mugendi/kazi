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
var KAZI_server= config.server || 'http://localhost:'+port
var KAZI_server= config.server || 'http://23.254.129.120:'+port

/*Categoright

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
*/


/*twitter.tracking.update_users
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
*/

/*

jobs.push(
	{				
		name:'twitter.tracking.track',
		id:'twitter:1:ecitizenke',
		
		job_id:'2.ecitizenke', //should be an interger but can be postfixed with '.Something.AnotherSomething'
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
*/

var jobToKill=
	{				
		id:'twitter.tracking.trending',
		ttl:5
	}



var post={
		url:KAZI_server+'/killJob', 
		form: jobToKill
}



// //first register client
request.post(post, function(err,httpResponse,body){ 
	if(body){
		console.log(body)
	}			
});

