var Q = require('../index');

var options = {
  retries : 2,
  timeout : 10000
};

setInterval(queueJob, 5000);

function queueJob(){

  var job = {
    queue : "facebook",
    data : { a: Math.random()*1000, created_time : new Date() }
  };

  //queueJob and return results...
  Q.queueJob( job, options, function(res){
    console.log(JSON.stringify(res,0,4));
  });

}
