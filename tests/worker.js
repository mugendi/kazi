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
