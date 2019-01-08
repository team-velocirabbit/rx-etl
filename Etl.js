const { Observable } = require('rxjs');
const { count, tap, switchMap, flatMap, map, bufferCount } = require('rxjs/operators');
const fileExtension = require('file-extension');
const connectionString = require('connection-string');
const { MongoClient } = require('mongodb');
const { invert } = require('underscore');
const scheduler = require('node-schedule');
const path = require('path');
const extract = require('./extractors/extract');
const load = require('./loaders/load');
require('dotenv').config()
const sgEmail = require('@sendgrid/mail');
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
	}

	/**
	 * Collects extractor$ and adds it in Etl's state
	 * 
	 * @param {Observable} extractorFunction - extract function that streams data from input source
	 * @param {string} connectStrOrFilePath - file path of the extract file OR collection name of db
	 * @returns {this}
	 */
	addExtractors(extractorFunction, connectStrOrFilePath, collection) {
		// check to see that extract function matches file path extension
		const type = invert(extract)[extractorFunction].substring(4).toLowerCase();
		const fileExt = fileExtension(connectStrOrFilePath).toLowerCase();
		const dbType = connectionString(connectStrOrFilePath);
		
		if (((type === 'csv' || type === 'xml' || type === 'json') && (type !== fileExt)) 
			|| ((type === 'mongodb' || type === 'postgres') && (type !== dbType))) {
			this.reset();
			throw new Error("please make sure extract function matches file type! \n");
		}

		// retrieve extractor observable from connectStrOrFilePath
		let extractor$ = extractorFunction(connectStrOrFilePath, collection);
		// buffer the observable to collect 1000 at a time
		extractor$ = extractor$.pipe(bufferCount(1000, 1000));
		// validate extractor$. If not valid, then reset Etl's state and throw error
		if (!(extractor$ instanceof Observable)) {
			this.reset();
			throw new Error("extractor function invalid\n See docs for more details.\n");
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
				throw new Error("argument must be an array of functions!\n See docs for more details.\n")
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
	 * @param {string} collectionNameOrFileName - collection name of load db OR file name of output
	 * @param {string} connectStrOrFilePath - connect string to load db OR file path of output file
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
		if ((type === 'mongodb' || type === 'postgres') && 
			((typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0) || 
			(typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0))) {
				this.reset();
				throw new Error("database loaders must provide connection string AND collection / table name in the parameter!\n");
		} else if (type === 'mongodb' || type === 'postgres') {
			this.type = 'db';
			this.connectionString = connectStrOrFilePath;
			this.collectionName = collectionNameOrFileName ? collectionNameOrFileName : ('etl_output');
		} 
		// make sure filename is provided if loading to flat file
		if ((type === 'csv' || type === 'xml' || type === 'json') &&
			((connectStrOrFilePath && typeof connectStrOrFilePath !== 'string') 
			|| (collectionNameOrFileName && typeof collectionNameOrFileName !== 'string'))) {
				this.reset();
				throw new Error("flatfile loaders must provide valid output file path and/or file name!\n");
		} else if (type === 'csv' || type === 'xml' || type === 'json') {
			// make sure appropriate output file was provided if given one (and not the default 'etl_output')
			if (collectionNameOrFileName && (collectionNameOrFileName !== 'etl_output')) {
				if (type === 'csv' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'csv') {
					throw new Error("loading to csv requires output file to be of type '.csv'!\n")
				}
				if (type === 'xml' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'xml') {
					throw new Error("loading to xml requires output file to be of type '.xml'!\n")
				}
				if (type === 'json' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'json') {
					throw new Error("loading to json requires output file to be of type '.json'!\n")
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
	 * Combines the extractor$, transformers, and loader in the state by piping each to one another
	 * and stores an Observable (containing the entire ETL process) in the state
	 * 
	 * @returns {this}
	 */
	combine() {
		// ensure that a previous Etl process (an Observable) does not exist, and if so, throw an error
		if (this.observable$ !== null) 
			throw new Error('Failed to combine. Please make sure previous ETL process does not exist and try using the .reset() method\n');
		// create a new observable from the extractor$ observable, and complete the extractor$ observable
		this.observable$ = this.extractor$;
		// pipe each event through a transformer function
		for (let i = 0; i < this.transformers.length; i += 1) {
			this.observable$ = this.observable$.pipe(map(data => {
				const result = [];
				data.forEach(d => result.push(this.transformers[i](d)));
				return result;
			}));
		}
		// check if loader type is flatfile or db and pipe each event through the loader function
		if (this.type === 'flatfile') {
			this.observable$ = this.observable$.pipe(map(data => this.loader(data, this.outputFilePath, this.outputFileName, this.initialWrite++)));
		} else if (this.type === 'db') {
			this.observable$ = this.observable$.pipe(map(data => this.loader(data, this.connectionString, this.collectionName)));
		}
		return this;
	}

	/**
	 * Subscribes to the Observable stored in Etl's state that encapsulates the entire Etl process
	 * 
	 * @param {boolean} startNow - value that indicates whether user wants job started right away or not
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
			// schedule job and store scheduled job in Etl's state, so that it's accessible to cancel/modify
			this.cronList.forEach(cron => {
				const job = scheduler.scheduleJob(
					cron, 
					() => {
						// reset initialWrite so that it overwrites file
						this.initialWrite = 0;
						this.observable$.subscribe(	
							null, 
							(err) => { throw new Error('unable to start etl process.\n', err) },
							null
						);
					}
				);
				this.schedule.push(job);
			});
		}
		// start job by default unless user specifies otherwise
		if ((startNow && this.cronList.length !== 0) || this.cronList.length === 0) {
			this.observable$.subscribe(	
				null, 
				(err) => { throw new Error('unable to start etl process.\n', err) },
				null
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
		return this;
	}

	/**
	 * Simple method that encapsulates the three different methods to add extractor$, transformers, and loader
	 * into a simple function that adds appropriate functions and methods to Etl's state
	 * 
	 * @param {string} extractString - name of file to extract from
	 * @param {function} callback - array of transform functions
	 * @param {string} connectStrOrFilePath - connect string of load db OR file path of output file
	 * @param {string} collectionNameOrFileName - collection name of load db OR file name of output file
	 * @returns {this}
	 */
	simple(extractString, callback, connectStrOrFilePath, collectionNameOrFileName = 'etl_output') {
		if (extractString === undefined || typeof extractString !== 'string' || extractString.length === 0) 
			throw new Error('first parameter of simple() must be a string and cannot be empty!');
		if (callback === undefined || !(callback instanceof Array)) 
			throw new Error('second parameter of simple() must be a function and cannot be empty!');
		if (connectStrOrFilePath === undefined || typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0) 
			throw new Error('third parameter of simple() must be a string and cannot be empty!');
		if (collectionNameOrFileName !== 'etl_output' && (typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0)) 
			throw new Error('fourth parameter of simple() must be a string!');
		// add valid callbacks to the list of transformers in state
		callback.forEach(cb => this.transformers.push(cb));
		/* EXTRACT: check extractString to choose appropriate extractor */
		if (fileExtension(extractString).toLowerCase() === 'csv') this.extractor$ = extract.fromCSV(extractString);
		if (fileExtension(extractString).toLowerCase() === 'json') this.extractor$ = extract.fromJSON(extractString);
		if (fileExtension(extractString).toLowerCase() === 'xml') this.extractor$ = extract.fromXML(extractString);
		if (connectionString(connectStrOrFilePath).protocol === 'mongodb') this.extractor$ = extract.fromMongoDB(extractString, collectionNameOrFileName);
		if (connectionString(connectStrOrFilePath).protocol === 'postgres') this.extractor$ = extract.fromPostgres(extractString, collectionNameOrFileName);
		// buffer the observable to collect 99 at a time
		this.extractor$ = this.extractor$.pipe(bufferCount(1000, 1000));
		let type = invert(load)[loader].substring(2).toLowerCase();
		// make sure user specifies output filename to be able to add appropriate loader
		if ((type === 'csv' || type === 'json' || type === 'xml') 
			&& fileExtension(collectionNameOrFileName).toLowerCase() === 'etl_output') {
			throw new Error("please specify output file name ending in: '.csv', '.xml', '.json'!\n");
		}
		if ((type === 'mongodb' || type === 'postgres') 
			&& (connectionString(connectStrOrFilePath).protocol !== 'mongodb' 
			&& connectionString(connectStrOrFilePath).protocol !== 'postgres')) {
			throw new Error("please specify connection string if trying to connect to database!\n");
		}
		// LOAD: check input to load to appropriate output source
		if (fileExtension(collectionNameOrFileName).toLowerCase() === 'csv') this.loader = load.toCSV;
		if (fileExtension(collectionNameOrFileName).toLowerCase() === 'json') this.loader = load.toJSON;
	  if(fileExtension(collectionNameOrFileName).toLowerCase() === 'xml') this.loader = load.toXML;
		if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'postgres') {
			this.loader = load.toPostgres;
			this.connectionString = connectStrOrFilePath;
			this.collectionName = collectionNameOrFileName;
		}
	  if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'mongodb') {
			this.loader = load.toMongoDB;
			this.connectionString = connectStrOrFilePath;
			this.collectionName = collectionNameOrFileName;
		}
		return this;
	}

  /**
	 * Method for sending SendGrid email notifications upon job completion
	 *
	 * @param {object} message - object containing the necessary info for sending a SendGrid email notification
	 * @returns {this}
	 */
  addEmailNotification(message) {
    sgEmail.setApiKey(process.env.SENDGRID_API_KEY);
    const msg = {
      to: message.to,
      from: message.from,
      subject: message.subject,
      text: message.text,
      html: message.html,
    };
    sgEmail.send(msg);
    return this;
  }

  /**
   * Method for sending Twilio text notifications upon job completion
   *
   * @param {object} message - object containing the necessary info for sending a Twilio text notification
   * @returns {this}
   */
  addTextNotification(message) {
    client.messages.create({
      from: process.env.TWILIO_PHONE_NUMBER,
      to: message.to,
      body: message.body,
    });
    return this;
	}
	
	/**
	 * Aggregate schedules for job in an array in Etl's state
	 * 
	 * @param {string} cron - schedule in cron-format used to add schedule for job
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
}

module.exports = Etl;
