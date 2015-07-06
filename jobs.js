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

var post={
		url:'http://localhost:2016/queueJob', 
		form: jobs
}


console.log(post);


// //first register client
request.post(post, function(err,httpResponse,body){ 
	if(body){
		console.log(body)
	}
			
});

