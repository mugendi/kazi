var Q = require('../index');

var options = { retries : 2, timeout : 10000 };
var queue = 'facebook';

//
Q.requestJob( queue, options, function( job, done, client ){

  console.log(job.data);
  //use data to execute job, ideally all required job variables and instructions should be saved here
  //....

  //close Client if need be
  // client.close();

  //finish job
  done();

  //update job & reschedule
  //set jobId of job....
  // job.data.jobId = 'search:user:365374o4809570';

  job.data.updated_time = new Date();

  var newJob = {
    queue : "facebook",
    data : job.data
  };

  //requeue job
  Q.queueJob( newJob, options, function(res){
    console.log(JSON.stringify(res,0,4));
  });

});
