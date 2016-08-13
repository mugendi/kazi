var Q = require('../index');

var options = {
  retries : 2,
  timeout : 10000
};

setInterval(queueJob, 5000);

function queueJob(){

  var job = {
    queue : "facebook",
    // jobId : 'search:user:365374o4809570',
    // delay : 60 * 1000,
    schedule : "every friday at 7 pm",
    data : { a: Math.random()*1000, created_time : new Date() }
  };

  // queueJob and return results...
  Q.queueJob( job, options, function(res){
    // console.log(JSON.stringify(res,0,4));
  });

}
