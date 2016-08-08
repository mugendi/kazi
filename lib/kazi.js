
//load main module
const Queue = require('bee-queue');
//Joi for json schema validation
const Joi = require('joi');
//async
const async = require('async');
//arrify job objects
const arrify = require('arrify');

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
    catchExceptions : Joi.boolean()
});

var jobSchema = Joi.object().keys({
    queue: Joi.string().alphanum().required(),
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

  addQueue.process(cb);

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

  //async through each array object
  async.eachLimit(jobArray, 1, function( job, next ){

    if( (errors = Joi.validate( job, jobSchema)) && errors.error ){
      throw new Error(errors.error);
    }

    //create queue
    var addQueue = new Queue(job.queue, options);
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
    //save job & callback
    newJob.save(function(error, job){
      delete job.queue;
      //make array of all job responses
      jobs.push(job);
      //next job
      next();

    });

  }, function(){ //when done with all the jobs
    //callback
    cb(jobs);
  });

}

module.exports = {
  queueJob, requestJob
};
