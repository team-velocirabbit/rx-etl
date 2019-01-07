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

/** 
 * Class that stores the extractor, transformers, and loader, 
 * and combines and executes the ETL process through streaming, using rxjs Observables
 * */
class Etl {
	/**
	 * initiates and stores initial values of the state that stores all contents of ETL
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
	 * @param {Observable} extractor$ - An observable that reads and streams the data from input source
	 * @param {string} filePath - The file path of the extract file
	 * @returns {this}
	 */
	addExtractors(extractorFunction, filePath) {
		// check to see that extract function matches filePath extension
		const type = invert(extract)[extractorFunction].substring(4).toLowerCase();
		const fileExt = fileExtension(filePath).toLowerCase();
		if (type !== fileExt) {
			this.reset();
			return console.error("Error: please make sure extract function matches file type! \n");
		}
		// retrieve extractor observable from filePath
		let extractor$ = extractorFunction(filePath);
		// buffer the observable to collect 99 at a time
		extractor$ = extractor$.pipe(bufferCount(1000, 1000));
		// validate extractor$. If not valid, then reset Etl's state and throw error
		if (!(extractor$ instanceof Observable)) {
			this.reset();
			return console.error("Error: extractor function invalid\n See docs for more details.\n");
		} else {
			this.extractor$ = extractor$;
		}
		return this;
	}

	/**
	 * Collects transformer(s) and stores it in the state of Etl
	 * 
	 * @param {function} transformers - array of functions that transform the source data
	 * @returns {this}
	 */
	addTransformers(...transformers) {
		// validate each function in transformers. If not valid, then reset Etl's state and throw error
		for (let i = 0; i < transformers.length; i += 1) {
			if (!(transformers[i] instanceof Function)) {
				this.reset();
				return console.error("Error: transformer functions must be of class 'Transformers'\n See docs for more details.\n")
			} else {
				this.transformers.push(transformers[i]);
			}
		}
		return this;
	}

	/**
	 * Collects loader function and database connection strings and stores it in the state of Etl
	 * 
	 * @param {function} loader - One (or many) functions that transforms the source data
	 * @param {string} collectionNameOrFileName - collection name (optional) OR file name, 'etl_output' by default
	 * @param {string} connectStrOrFilePath - connect string to the load db OR file path if loading to flatfile
	 * @returns {this}
	 */
	addLoaders(loader, collectionNameOrFileName, connectStrOrFilePath) {
		// parse the loader to function to check if loader is to a flat file or db
		let type = '';
		// validate params. If not valid, then reset the Etl's state and throw error
		if (!(loader instanceof Function)) {
			this.reset();
			return console.error("Error: loader functions must be of class 'Loaders'\n See docs for more details.\n")
		} else {
			// get either CSV, XML, JSON, MONGODB, POSTGRES from name of function
			type = invert(load)[loader].substring(2).toLowerCase();
		}
		// makes sure that connectionString and collectionName is provided if loader is to a database
		if ((type === 'mongodb' || type === 'postgres') && 
			((typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0) || 
			(typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0))) {
				this.reset();
				return console.error("Error: database loaders must provide connection string AND collection / table name in the parameter!\n");
		} else if (type === 'mongodb' || type === 'postgres') {
			this.type = 'db'
			this.connectionString = connectStrOrFilePath;
			this.collectionName = collectionNameOrFileName ? collectionNameOrFileName : ('etl_output');
		} 
		// make sure filename is provided if loading to flat file
		if ((type === 'csv' || type === 'xml' || type === 'json') &&
			((connectStrOrFilePath && typeof connectStrOrFilePath !== 'string') 
			|| (collectionNameOrFileName && typeof collectionNameOrFileName !== 'string'))) {
				this.reset();
				return console.error("Error: flatfile loaders must provide valid output file path and/or file name!\n");
		} else if (type === 'csv' || type === 'xml' || type === 'json') {
			// make sure appropriate output file was provided if given one (and not the default 'etl_output')
			if (collectionNameOrFileName && (collectionNameOrFileName !== 'etl_output')) {
				if (type === 'csv' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'csv') {
					return console.error("Error: loading to csv requires output file to be of type '.csv'!\n")
				}
				if (type === 'xml' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'xml') {
					return console.error("Error: loading to xml requires output file to be of type '.xml'!\n")
				}
				if (type === 'json' && fileExtension(collectionNameOrFileName).toLowerCase() !== 'json') {
					return console.error("Error: loading to json requires output file to be of type '.json'!\n")
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
			return console.error('Error: Failed to combine. Please make sure previous ETL process does not exist and try using the .reset() method\n');
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
	 * @returns {string} message - send back a message declaring success or failure
	 */
	start() {
		if (this.observable$ === null) 
			return console.error('Error: Failed to start. Please make sure extractors, transformers, loaders were added and combined using the .combine() method.\n');

		if (this.schedule.length === 0) {
			// close the database connection upon completion, return error if error is thrown
			this.observable$.subscribe(	
				null, 
				(err) => console.error('Error: unable to start etl process.\n', err),
				null
			);
		}
		else {
			this.schedule.forEach(cron => {

			})
		}
		return 'Successfully Completed';
	}

	/**
	 * Resets the Etl's state to default values
	 * 
	 * @returns {this}
	 */	
	reset() {
		this.extractor$ = null;
		this.transformers = [];
		this.loader = null;
		this.observable$ = null;
		return this;
	}

	/**
	 * Simple method that encapsulates the three different methods to add extractor$, transformers, and loader
	 * into a simple function that adds appropriate functions and methods to Etl's state
	 * 
	 * @returns {this}
	 */
	simple(extractString, callback, connectStrOrFilePath, collectionNameOrFileName = 'etl_output') {
		// validate input
		if (extractString === undefined || typeof extractString !== 'string' || extractString.length === 0)
			return console.error('Error: first parameter of simple() must be a string and cannot be empty!');
		if (callback === undefined || !callback instanceof Array) 
			return console.error('Error: second parameter of simple() must be a function and cannot be empty!');
		if (connectStrOrFilePath === undefined || typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0) 
			return console.error('Error: third parameter of simple() must be a string and cannot be empty!');
		// add valid callbacks to the list of transformers in state
		callback.forEach(cb => this.transformers.push(cb));
		/* EXTRACT: check extractString to choose appropriate extractor */
		if (fileExtension(extractString).toLowerCase() === 'csv') this.extractor$ = extract.fromCSV(extractString);
		if (fileExtension(extractString).toLowerCase() === 'json') this.extractor$ = extract.fromJSON(extractString);
		if (fileExtension(extractString).toLowerCase() === 'xml') this.extractor$ = extract.fromXML(extractString);
		// buffer the observable to collect 99 at a time
		this.extractor$ = this.extractor$.pipe(bufferCount(1000, 1000));
		// make sure user specifies output filename to be able to add appropriate loader
		if (fileExtension(collectionNameOrFileName).toLowerCase() === 'etl_output') {
			return console.error("Error: to use simple, please specify output file name ending in: '.csv', '.xml', '.json'!\n");
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
	 * Aggregate shedules for job in an array in ETL's state
	 * 
	 * @returns {this}
	 */
	addSchedule(...cron) {
		// validate each cron and store it in state
		for (let i = 0; i < cron.length; i += 1) {
  		let parsedCron = cron[i].split(' ');
  		if (parsedCron.length !== 5) return console.error('Error: cron format is invalid! \n');
  		parsedCron = parsedCron.join(' ').replace(/[*0-9]/g, '').trim();
  		if (parsedCron.length !== 0) return console.error('Error: cron format is invalid. \n');
  		this.cronList.push(cron[i]);
		}
		return this;
	}

module.exports = Etl;