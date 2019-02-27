const { Observable } = require('rxjs');
const { map, bufferCount } = require('rxjs/operators');
const fileExtension = require('file-extension');
const connectionString = require('connection-string');
const { invert } = require('underscore');
const scheduler = require('node-schedule');
const sgEmail = require('@sendgrid/mail');
const extract = require('./extractors/extract');
const load = require('./loaders/load');
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
    if (((type === 'csv' || type === 'xml' || type === 'json') && (type !== fileExt)) 
      || ((type === 'mongodb' || type === 'postgres') && (type !== dbType))) {
      this.reset();
      throw new Error('please make sure extract function matches file type! \n');
    }
    // retrieve extractor observable from connectStrOrFilePath
    let extractor$ = extractorFunction(connectStrOrFilePath, collection);
    // buffer the observable to collect 1000 at a time
    extractor$ = extractor$.pipe(bufferCount(1000, 1000));
    // validate extractor$. If not valid, then reset Etl's state and throw error
    if (!(extractor$ instanceof Observable)) {
      this.reset();
      throw new Error('extractor function invalid\n See docs for more details.\n');
    } else {
      this.extractor$ = extractor$;
    }
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
      if (!(transformers[i] instanceof Function)) {
        this.reset();
        throw new Error('argument must be an array of functions!\n See docs for more details.\n')
      } else {
        this.transformers.push(transformers[i]);
      }
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
    if (!(loader instanceof Function)) {
      this.reset();
      throw new Error("loader functions must be of class 'Loaders'\n See docs for more details.\n")
    } else {
      // get either CSV, XML, JSON, MONGODB, POSTGRES from name of function
      type = invert(load)[loader].substring(2).toLowerCase();
    }
    // makes sure that connectionString and collectionName is provided if loader is to a database
    if ((type === 'mongodb' || type === 'postgres')
      && ((typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0)
      || (typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0))) {
      this.reset();
      throw new Error('database loaders must provide connection string AND collection / table name in the parameter!\n');
    } else if (type === 'mongodb' || type === 'postgres') {
      this.type = 'db';
      this.connectionString = connectStrOrFilePath;
      this.collectionName = collectionNameOrFileName ? collectionNameOrFileName : ('etl_output');
    }
    // make sure filename is provided if loading to flat file
    if ((type === 'csv' || type === 'xml' || type === 'json')
    && ((connectStrOrFilePath && typeof connectStrOrFilePath !== 'string')
    || (collectionNameOrFileName && typeof collectionNameOrFileName !== 'string'))) {
      this.reset();
      throw new Error('flatfile loaders must provide valid output file path and/or file name!\n');
    } else if (type === 'csv' || type === 'xml' || type === 'json') {
      // make sure appropriate output file was provided
      if (collectionNameOrFileName && (collectionNameOrFileName !== 'etl_output')) {
        if (type === 'csv' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'csv') {
          throw new Error("loading to csv requires output file to be of type '.csv'!\n");
        }
        if (type === 'xml' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'xml') {
          throw new Error("loading to xml requires output file to be of type '.xml'!\n");
        }
        if (type === 'json' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'json') {
          throw new Error("loading to json requires output file to be of type '.json'!\n");
        }
      }
      this.type = 'flatfile';
      // if not provided, by default, outputFileName and outFilePath are given default values
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
    if (this.observable$ !== null) throw new Error('Failed to combine. Please make sure previous ETL process does not exist and try using the .reset() method\n');
    // create a new observable from the extractor$ observable,
    // and complete the extractor$ observable
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
    if (typeof startNow !== 'boolean') {
      throw new Error("invalid arg to .start() method! Please make sure arg is type 'boolean'!\n");
    }
    // check to make sure user invoked .combine() before invoking start method
    if (this.observable$ === null) {
      throw new Error('Failed to start. Please make sure extractors, transformers, loaders were added and combined using the .combine() method.\n');
    }
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
    if (!extractString || typeof extractString !== 'string' || extractString.length === 0) {
      throw new Error('first parameter of simple() must be a string and cannot be empty!');
    }
    if (extractCollection === undefined || (typeof extractCollection !== 'string' && extractCollection !== null) 
      || (typeof extractCollection === 'string' && extractCollection.length === 0)) {
      throw new Error('second parameter of simple() must be a string and cannot be empty! Assign to "null" if not needed.');
    }
    if (callback === undefined || !(callback instanceof Array)) {
      throw new Error('third parameter of simple() must be an array and cannot be empty!' 
      + ' Insert function into array and pass in.');
    }
    if (connectStrOrFilePath === undefined || typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0) {
      throw new Error('fourth parameter of simple() must be a string and cannot be empty!');
    } 
    if (collectionNameOrFileName !== 'etl_output' && (typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0)) {
      throw new Error('fifth parameter of simple() must be a string!');
    } 
    // add valid callbacks to the list of transformers in state
    callback.forEach(cb => this.transformers.push(cb));
    /* EXTRACT: check extractString to choose appropriate extractor */
    if (fileExtension(extractString).toLowerCase() === 'csv') this.extractor$ = extract.fromCSV(extractString);
    if (fileExtension(extractString).toLowerCase() === 'json') this.extractor$ = extract.fromJSON(extractString);
    if (fileExtension(extractString).toLowerCase() === 'xml') this.extractor$ = extract.fromXML(extractString);
    if (connectionString(extractString).protocol && connectionString(extractString).protocol === 'mongodb') {
      if (extractCollection === null) throw new Error('extracting from database requires collection name.');
      this.extractor$ = extract.fromMongoDB(extractString, extractCollection);
    }
    if (connectionString(extractString).protocol && connectionString(extractString).protocol === 'postgres') {
      if (extractCollection === null) throw new Error('extracting from database requires table name.');
      this.extractor$ = extract.fromPostgres(extractString, collectionNameOrFileName);
    }
    // buffer the observable to collect 1000 at a time
    this.extractor$ = this.extractor$.pipe(bufferCount(1000, 1000));
  
    // LOAD: check input to load to appropriate output source
    if (fileExtension(collectionNameOrFileName).toLowerCase() === 'csv') {
      this.loader = load.toCSV;
      this.outputFilePath = connectStrOrFilePath;
      this.outputFileName = collectionNameOrFileName;
      this.type = 'flatfile';
    } else if (fileExtension(collectionNameOrFileName).toLowerCase() === 'json') {
      this.loader = load.toJSON;
      this.outputFilePath = connectStrOrFilePath;
      this.outputFileName = collectionNameOrFileName;
      this.type = 'flatfile';
    } else if (fileExtension(collectionNameOrFileName).toLowerCase() === 'xml') {
      this.loader = load.toXML;
      this.outputFilePath = connectStrOrFilePath;
      this.outputFileName = collectionNameOrFileName;
      this.type = 'flatfile';
    } else if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'postgres') {
      this.loader = load.toPostgres;
      this.connectionString = connectStrOrFilePath;
      this.collectionName = collectionNameOrFileName;
      this.type = 'db';
    } else if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'mongodb') {
      this.loader = load.toMongoDB;
      this.connectionString = connectStrOrFilePath;
      this.collectionName = collectionNameOrFileName;
      this.type = 'db';
    } else throw new Error('invalid load file name / collection name given. Please make sure to add the extension to the new file name.');
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
    // validate each cron and store it in state
    for (let i = 0; i < cron.length; i += 1) {
      let parsedCron = cron[i].split(' ');
      if (parsedCron.length < 5 || parsedCron.length < 6) throw new Error('cron format is invalid! \n');
      parsedCron = parsedCron.join(' ').replace(/[*0-9]/g, '').trim();
      if (parsedCron.length !== 0) throw new Error('cron format is invalid. \n');
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
