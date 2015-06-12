var _ = require('lodash'),
	jobSetKey='KULA:jobs',
	jobHashKey='KULA:existing'
	clientsHashKey='KULA:clients'
	;

var request = require('request');

var jobs=[];
var jobTypes=[
		'getTitles'
	];


jobTypes.forEach(function(name,id){
	
	jobs.push(
		{
			priority:'normal',
			id:id+1,
			name:name,
			terminateJobAfter: (5*1000*60) //5 mins
		}
	);
	
});



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

