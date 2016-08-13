/*jshint -W104*/
/*jshint -W014*/

//load main module
const Queue = require('./bee-queue/queue');
//Joi for json schema validation
const Joi = require('joi');
//async
const async = require('async');
//arrify job objects
const arrify = require('arrify');

const hadithi = require('hadithi');
const ms = require('ms');
const stringify = require('json-stringify-safe');
const parseTime = require('parse-messy-time');

//load configs
const defaults = require('../config.json');

//expected JSON schemas
var optionsSchema = Joi.object().keys({
    retries : Joi.number(),
    timeout : Joi.number(),
    prefix : Joi.string(),
    stallInterval : Joi.number(),
    redis : Joi.object(),
    getEvents : Joi.boolean(),
    isWorker : Joi.boolean(),
    sendEvents : Joi.boolean(),
    removeOnSuccess : Joi.boolean(),
    catchExceptions : Joi.boolean(),
    debug : true
});

var jobSchema = Joi.object().keys({
    queue: Joi.string().required(),
    jobId: Joi.string(),
    delay : Joi.any(),
    schedule : Joi.string(),
    data: Joi.object().required()
});


function requestJob(queue, options, cb){
  //asign defaults to options passed...
  options = Object.assign( defaults, options || {});
  //ensure we have a callback
  cb = typeof cb == 'function' ? cb : function(){};

  if( (errors = Joi.validate( options, optionsSchema)) && errors.error ){
    throw new Error(errors.error);
  }

  if(typeof queue !== 'string'){ throw new Error('Queue Must be a string!'); }

  // console.log(queue);
  //create queue
  var addQueue = new Queue(queue, options);

  addQueue.process(function(job,done){
    cb( job, done, addQueue);
  });

}

function scheduledJobs(options, cb){
  //asign defaults to options passed...
  options = Object.assign( defaults, options || {});

  var addQueue = new Queue('*', defaults);
  var schedule_key = options.prefix+":scheduled";
  var now = new Date().getTime();
  var jobs = [];

  addQueue.client.zrangebyscore(schedule_key, 0, now ,function(err,res){

    //loop thru jobs
    if(!err){

      res.forEach(function(jobStr){
        try {
          jobs.push(JSON.parse(jobStr));
        } catch (e) { console.log(e); }
      });

    }

    //remove jobs within range...
    if(jobs.length){
      // zremrangebyscore
      addQueue.client.zremrangebyscore(schedule_key, 0, now ,function(err,res){
        // console.log(res);
      });
    }

    //callback with jobs...
    cb(jobs);

  });

}

//queue function
function queueJob(jobArray, options, cb){
  //asign defaults to options passed...
  options = Object.assign( defaults, options || {});
  //ensure we have a callback
  cb = typeof cb == 'function' ? cb : function(){};

  if( (errors = Joi.validate( options, optionsSchema)) && errors.error ){
    throw new Error(errors.error);
  }

  //ensure array
  jobArray = arrify(jobArray);
  var jobs = [];
  var schedule_key = options.prefix+":scheduled";

  //async through each array object
  async.eachLimit(jobArray, 1, function( job, next ){
    //create queue
    var addQueue = new Queue(job.queue, options);

    if(!job){
      addQueue.close();
      //next job
      next();
    }

    if( (errors = Joi.validate( job, jobSchema)) && errors.error ){
      throw new Error(errors.error);
    }

    //if job is to be delayed
    if( job.delay ){

      var delay = /[^0-9\.]/.test(job.delay)
                    ? Number( ms(job.delay).replace(/[^0-9\.]/,''))
                    : job.delay ;

      //set delay to a timestamp in the future
      delay+= new Date().getTime();

      //have delay in data object
      job.data.delay = job.delay;
      //remove delay key
      delete job.delay;

      if(options.debug){
        logJob(job, 'Scheduling Job For: ' + new Date(delay).toUTCString() );
      }


      // console.log( delay );
      addQueue.client.zadd(schedule_key, delay, stringify(job) ,function(err,res){
        // console.log(err, res)
        job.data.status = "Scheduled for:" + new Date(delay);
        jobs.push(job);
        //
        addQueue.close();
        //next job
        next();

      });

    }
    else if( job.schedule ){
      // job.schedule = 'JOB ' + job.schedule;
      var schedule = parseTime(job.schedule);
      var repeatSchedule = /every|each/g.test(job.schedule);
      var now = new Date().getTime();
      var delay = new Date(schedule).getTime();

      //shedules must be in the future
      if( now<delay ){
        //have schedule in data object
        job.data.schedule = job.schedule;

        //remove schedule from job...
        if(!repeatSchedule){ delete job.schedule; }
        // console.log(next)

        if(options.debug){
          logJob(job, 'Scheduling Job For: ' + new Date(delay).toUTCString() );
        }

        // console.log( delay );
        addQueue.client.zadd(schedule_key, delay, stringify(job) ,function(err,res){
          // console.log(err, res)
          job.data.status = "Scheduled for:" + new Date(delay);
          jobs.push(job);
          //
          addQueue.close();
          //next job
          next();
        });
      }
      else{
        throw new Error("Could not schedule job. Wrong schedule: " + job.schedule);
      }
    }
    else{

      //add job data
      var newJob = addQueue.createJob(job.data);
      //add timeout
      if(options.hasOwnProperty('timeout') && (timeout=parseInt(options.timeout)) && timeout){
        newJob.timeout(timeout);
      }
      //add number of retries
      if(options.hasOwnProperty('retries') && (retries=parseInt(options.retries)) && retries){
        newJob.retries(retries);
      }

      if(job.hasOwnProperty('jobId')){
        //only save if job doesnt exist
        addQueue.getJob(job.jobId, function (err, savedJob) {

          if(savedJob.status===undefined){
            //
            if(options.debug){
              logJob(job, 'Saving Job: ');
            }

            //add id using setId
            newJob.setId(job.jobId);

            //have job
            newJob.save(function(error, job){
              delete job.queue;
              //make array of all job responses
              jobs.push(job);
              //close redis connection
              addQueue.close();
              //next job
              next();
            });

          }
          else{
            if(options.debug){
              logJob(job, 'Skipping Duplicate Job: ');
            }
            //close redis connection
            addQueue.close();
            //next job
            next();
          }

        });

      }
      else{
        if(options.debug){
          logJob(job, 'Saving Job: ');
        }
        //save job anyway...
        newJob.save(function(error, job){
          delete job.queue;
          //make array of all job responses
          jobs.push(job);
          //close redis connection
          addQueue.close();
          //next job
          next();
        });

      }

    }

  }, function(){ //when done with all the jobs
    // console.log('sksbsb')
    //callback
    cb(jobs);

  });

}



Array.prototype.max = function() {
  return Math.max.apply(null, this);
};

Array.prototype.min = function() {
  return Math.min.apply(null, this);
};

function logJob(jobs, message){

  jobs = arrify(jobs);

  var storyline = [
    {hadithiType:'title', hadithiColor: "blue", hadithiContent: "JOB STATUS"},
    message
  ];

  jobs.forEach(function(job){

    storyline.push({
      queue : ( job.queue || job.data.queue ),
      jobId :  ( job.jobId || job.data.jobId ),
      delay :  ( job.data.delay ? ms(job.data.delay) : 'N/A' ),
      schedule :  ( job.schedule || job.data.schedule ),
      jobId :  ( job.jobId || job.id || '[incremental]' )
    });

  });



  hadithi.log(storyline);

}

module.exports = {
  queueJob, requestJob, scheduledJobs
};
