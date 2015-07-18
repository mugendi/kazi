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


var trends=['#kotvsuot','#stopkiderogoons']

trends.forEach(function(trend,i){

	jobs.push(
		{				
			name:'twitter.tracking.track',
			id:'twitter:trending:'+trend,
			job_id:'trending:'+trend,
			_index:'tracking',
			_type: 'twitter',
			end_point:'streaming',
			terms:[trend],
			delay:(5*i),
			//set ttl to 3 hours
			ttl:(3600*12),
			data:{			
				max_id:0,
				since_id:0,	
				type:'trending'					
			}
		}
	);

});






var post={
		url:'http://23.254.129.120:2500/queueJob', 
		form: jobs
}


console.log(post);


// //first register client
request.post(post, function(err,httpResponse,body){ 
	if(body){
		console.log(body)
	}
			
});

