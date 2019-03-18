const { Observable } = require('rxjs');
const { map, bufferCount } = require('rxjs/operators');
const fileExtension = require('file-extension');
const connectionString = require('connection-string');
const { invert } = require('underscore');
const scheduler = require('node-schedule');
const sgEmail = require('@sendgrid/mail');
const extract = require('./extractors/extract');
const load = require('./loaders/load');
const validate = require('./validators/validate');
require('dotenv').config();
const client = require('twilio')(
  process.env.TWILIO_ACCOUNT_SID,
  process.env.TWILIO_AUTH_TOKEN,
);

/**
 * Class that stores the extractor, transformers, and loader, 
 * and combines and executes the ETL process through streaming, using rxjs Observables
 * */
class Etl {
  /**
  * initiates and stores initial values of state that stores all contents of ETL
  */
  constructor() {
    this.extractor$ = null;
    this.transformers = [];
    this.loader = null;
    this.observable$ = null;
    this.connectionString = '';
    this.collectionName = '';
    this.outputFilePath = '';
    this.outputFileName = '';
    this.type = '';
    this.initialWrite = 0;
    this.schedule = [];
    this.cronList = [];
    this.text = null;
    this.email = null;
    this.nextJob = null;
  }

  /**
  * Collects extractor$ and adds it in Etl's state
  * 
  * @param {Observable} extractorFunction - extract function streaming data from input source
  * @param {string} connectStrOrFilePath - file path or connection string
  * @returns {this}
  */
  addExtractors(extractorFunction, connectStrOrFilePath, collection) {
    // check to see that extract function matches file path extension
    const type = invert(extract)[extractorFunction].substring(4).toLowerCase();
    const fileExt = fileExtension(connectStrOrFilePath).toLowerCase();
    const dbType = connectionString(connectStrOrFilePath).protocol;
    validate.extractorArgs(this, type, dbType, fileExt);
    // retrieve extractor observable from connectStrOrFilePath
    let extractor$ = extractorFunction(connectStrOrFilePath, collection);
    // buffer the observable to collect 1000 at a time
    extractor$ = extractor$.pipe(bufferCount(1000, 1000));
    // validate extractor$. If not valid, reset Etl's state and throw error
    validate.extractor$(this, extractor$);
    return this;
  }

  /**
  * Collects transformer(s) and stores it in Etl's state
  * 
  * @param {array} transformers - array of functions that transform the source data
  * @returns {this}
  */
  addTransformers(transformers) {
    // validate each transformer function in input array, and store valid functions to Etl's state
    for (let i = 0; i < transformers.length; i += 1) {
      if (validate.transformer(this, transformers[i])) this.transformers.push(transformers[i]);
    }
    return this;
  }

  /**
   * Collects loader function, collection name (if db) or file name (if flatfile), 
   * connection string (if db) or file path (if flatfile) and stores it in the Etl's state 
   * 
   * @param {function} loader - function that loads data to an output source
   * @param {string} collectionNameOrFileName - collection name or file name
   * @param {string} connectStrOrFilePath - connection string or file path
   * @returns {this}
   */
  addLoaders(loader, collectionNameOrFileName, connectStrOrFilePath) {
    let type = '';
    // get either CSV, XML, JSON, MONGODB, POSTGRES from name of function
    if (validate.loader(this, loader)) type = invert(load)[loader].substring(2).toLowerCase();
    // make sure appropriate args are provided for loader function
    validate.loaderArgs(this, type, connectStrOrFilePath, collectionNameOrFileName);
    if (type === 'mongodb' || type === 'postgres') {
      this.type = 'db';
      this.connectionString = connectStrOrFilePath;
      this.collectionName = collectionNameOrFileName ? collectionNameOrFileName : ('etl_output');
    }
    if (type === 'csv' || type === 'xml' || type === 'json') {
      // make sure appropriate output file was provided
      validate.loaderOutputFile(type, collectionNameOrFileName);
      this.type = 'flatfile';
      // if not provided, then outputFileName and outFilePath are given default values
      this.outputFileName = collectionNameOrFileName ? collectionNameOrFileName : ('etl_output' + '.' + type);
      this.outputFilePath = connectStrOrFilePath ? connectStrOrFilePath : __dirname;
    }
    this.loader = loader;
    return this;
  }

  /**
   * Combines the extractor$, transformers, and loader in the state by piping each to one 
   * another and stores an Observable (containing the entire ETL process) in the state 
   * 
   * @returns {this}
   */
  combine() {
    // ensure that a previous Etl process (an Observable) does not exist, and if so, throw an error
    validate.newEtlProcess(this);
    // create a new observable from the extractor$ observable, and complete the extractor$ observable
    this.observable$ = this.extractor$;
    // pipe each event through a transformer function
    for (let i = 0; i < this.transformers.length; i += 1) {
      this.observable$ = this.observable$.pipe(map((data) => {
        const result = [];
        data.forEach(d => result.push(this.transformers[i](d)));
        return result;
      }));
    }
    // check if loader type is flatfile or db and pipe each event through the loader function
    if (this.type === 'flatfile') {
      this.observable$ = this.observable$.pipe(map((data) => {
        this.loader(data, this.outputFilePath, this.outputFileName, this.initialWrite++);
      }));
    } else if (this.type === 'db') {
      this.observable$ = this.observable$.pipe(map((data) => {
        this.loader(data, this.connectionString, this.collectionName);
      }));
    }
    return this;
  }

  /**
   * Subscribes to the Observable stored in Etl's state that encapsulates the entire Etl process
   * 
   * @param {boolean} startNow - indicates if job is to be run when process starts
   * @returns {this}
   */
  start(startNow = true) {
    validate.beforeStart(this, startNow);
    // check if a schedule has been designated for job
    if (this.cronList.length !== 0) {
      // initial start if specified
      if (startNow) {
        this.observable$.subscribe(
          null,
          (err) => { throw new Error('unable to start etl process.\n', err) },
          () => onComplete(this.text, this.email, this.nextJob),
        );
      }
      // schedule job and store scheduled job in Etl's state
      this.cronList.forEach((cron) => {
        const job = scheduler.scheduleJob(
          cron, () => {      	
          // reset initialWrite so that it overwrites file
            this.initialWrite = 0;
            this.observable$.subscribe(	
              null,
              (err) => { throw new Error('unable to start etl process.\n', err); },
              () => onComplete(this.text, this.email, this.nextJob),
            );
          },
        );
        this.schedule.push(job);
      });
    }
    // start job by default unless user specifies otherwise
    else {
      this.observable$.subscribe(
        null,
        (err) => { throw new Error('unable to start etl process.\n', err); },
        () => {
          // reset initialWrite so that it overwrites file
          this.initialWrite = 0;
          this.observable$.subscribe(
            null,
            (err) => { throw new Error('unable to start etl process.\n', err); },
            () => onComplete(this.text, this.email, this.nextJob),
          );
        },
      );
    }
    return this;
  }

  /**
   * Resets Etl's state to default values
   * 
   * @returns {this}
   */
  reset() {
    this.extractor$ = null;
    this.transformers = [];
    this.loader = null;
    this.observable$ = null;
    this.connectionString = '';
    this.collectionName = '';
    this.outputFilePath = '';
    this.outputFileName = '';
    this.type = '';
    this.initialWrite = 0;
    this.schedule = [];
    this.cronList = [];
    this.nextJob = null;
    return this;
  }

  /**
   * Simple method that encapsulates the three different methods
   * to add extractor$ transformers, and loader into a simple function 
   * that adds appropriate functions and methods to Etl's state
   * 
   * @param {string} extractString - name of source to extract from (db connection str or file path)
   * @param {string} extractCollection - name of source collection/table to extract from
   * @param {array} callback - array of transform functions
   * @param {string} connectStrOrFilePath - connection string or file path to export to
   * @param {string} collectionNameOrFileName - collection name or file name to export to
   * @returns {this}
   */
  simple(extractString, extractCollection, callback, connectStrOrFilePath, collectionNameOrFileName = 'etl_output') {
    // check the arguments passed into simple before proceeding
    validate.simpleArgs(extractString, extractCollection, callback, connectStrOrFilePath, collectionNameOrFileName);
    // add valid callbacks to the list of transformers in state
    callback.forEach(cb => this.transformers.push(cb));
    /* EXTRACT: check extractString to choose appropriate extractor */
    this.simpleExtract(this, extractString, extractCollection, collectionNameOrFileName);
    // buffer the observable to collect 1000 at a time
    this.extractor$ = this.extractor$.pipe(bufferCount(1000, 1000));
    // LOAD: check input to load to appropriate output source
    validate.simpleLoad(this, connectStrOrFilePath, collectionNameOrFileName);
    return this;
  }

  /**
   * Method for storing SendGrid email notifications to send upon job completion
   * 
   * @param {object} message - contains information to send email
   * @returns {this}
   */
  addEmailNotification(message) {
    this.email = message;
    return this;
  }

  /**
   * Method for storing Twilio text notification to send upon job completion
   *
   * @param {object} message - contains information to send text
   * @returns {this}
   */
  addTextNotification(message) {
    this.text = message;
    return this;
  }

  /**
   * Aggregate schedules for job in an array in Etl's state
   * 
   * @param {string} cron - schedule in cron-format to schedule job
   * @returns {this}
   */
  addSchedule(...cron) {
    // check format of each cron and store it in state
    for (let i = 0; i < cron.length; i += 1) {
      validate.cron(cron[i]);
      this.cronList.push(cron[i]);
    }
    return this;
  }

  /**
   * Queue up next job to execute after current job
   * 
   * @param {object} job - Etl object containing next job to run
   * @param {boolean} startNow - indicates whether next job starts immediately
   * @returns {this}
   */
  next(job, startNow = true) {
    this.nextJob = { job, startNow };
    return this;
  }
}

/**
 * Helper method to send notifications and start next job
 * upon current job's completion
 * 
 * @param {object} text - contains information to send text
 * @param {object} email - contains information to send email
 * @param {object} nextJob - Etl object containing next job to run
 * @returns {this}
 */
function onComplete(text, email, nextJob) {
   // send text and/or email if specified
   if (text !== null) {
    client.messages.create({
      from: process.env.TWILIO_PHONE_NUMBER,
      to: text.to,
      body: text.body,
    });
  }
  if (email !== null) {
    sgEmail.setApiKey(process.env.SENDGRID_API_KEY);
    const msg = {
      to: email.to,
      from: email.from,
      subject: email.subject,
      text: email.text,
      html: email.html,
    };
    sgEmail.send(msg);
  }
  // start the next job on completion of current job
  if (nextJob) {
    const { job, startNow } = nextJob;
    job.start(startNow);
  }
  return;
}

module.exports = Etl;
