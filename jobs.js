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



var post={
		url:KAZI_server+'/queueJob', 
		form: jobs
}


console.log(post);


// //first register client
request.post(post, function(err,httpResponse,body){ 
	if(body){
		console.log(body)
	}
			
});

