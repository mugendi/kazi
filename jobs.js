var _ = require('lodash'),
	jobSetKey='KULA:jobs',
	jobHashKey='KULA:existing'
	clientsHashKey='KULA:clients'
	;

var request = require('request');

var jobs=[];
var jobTypes=[
		'twitter',
		'facebook'
	];


jobTypes.forEach(function(name,id){
	
	jobs.push(
		{
			priority:_.sample(['normal','high']) ,
			id:name+':'+id+':'+(new Date().getTime()), //Twitter:1:1
			name:name, //twitter
			terminateJobAfter: (5*1000*60), //5 mins
			delay:20
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

