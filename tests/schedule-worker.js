var Q = require('../index');
const chalk = require('chalk');

var options = { retries : 2, timeout : 10000 };


//use intervals to check for & run scheduled jobs

//start the process...
runScheduled();
var interval = 10000;

function runScheduled(){
  console.log(chalk.magenta("\nCheking for Scheduled Jobs..."));

  //get scheduled jobs
  Q.scheduledJobs(options, function( jobs ){
    console.log(chalk.magenta( jobs.length + ' Scheduled Jobs found...'));

    if(jobs.length){
      //queue jobs that now have immediate flag
      Q.queueJob( jobs, options, function(res){
        console.log(chalk.magenta( "\n" + res.length + " Jobs Added." ));
        setTimeout(runScheduled, interval);
      });
    }
    else{ setTimeout(runScheduled, interval); }

  });
}
