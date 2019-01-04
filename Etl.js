const { Observable } = require('rxjs');
const { count, tap, switchMap, flatMap, map, bufferCount } = require('rxjs/operators');
const fileExtension = require('file-extension');
const connectionString = require('connection-string');
const { MongoClient } = require('mongodb');
const { invert } = require('underscore');
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
		this.outputFilename = '';
		this.test = '',
		this.type = ''
	}

	/**
	 * Collects extractor$ and adds it in Etl's state
	 * 
	 * @param {Observable} extractor$ - An observable that reads and streams the data from input source
	 * @returns {this}
	 */
	addExtractors(extractorFunction, filepath) {
		// retrieve extractor observable from filepath
		let extractor$ = extractorFunction(filepath);

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
	 * @param {function} transformers - One (or many) functions that transforms the source data
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
	 * @param {string} connectStrOrFilename - connect string to the load db (Mongo or Postgres) OR filename if loading to flatfile
	 * @param {string} collectionName - The collection name (optional), 'etl_output' by default
	 * @returns {this}
	 */
	addLoaders(loader, connectStrOrFilename, collectionName = 'etl_output') {
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
			((typeof connectStrOrFilename !== 'string' || connectStrOrFilename.length === 0) || 
			(typeof collectionName !== 'string' || collectionName.length === 0))) {
				this.reset();
				return console.error("Error: database loaders must provide connection string AND collection / table name in the parameter!\n");
		} else if (type === 'mongodb' || type === 'postgres') {
			this.type = 'db'
			this.loader = loader;
			this.connectionString = connectStrOrFilename;
			this.collectionName = collectionName;
		} 

		// make sure filename is provided if loading to flat file
		if ((type === 'csv' || type === 'xml' || type === 'json') && 
			(typeof connectStrOrFilename !== 'string' ) ) {
				this.reset();
				return console.error("Error: flatfile loaders must provide output filename as the second argument!\n");
		} else if (type === 'csv' || type === 'xml' || type === 'json') {
			this.type = 'flatfile';
			this.loader = loader;
			this.outputFilename = connectStrOrFilename ? connectStrOrFilename : 'etl_output';
		}

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
			this.observable$ = this.observable$.pipe(map(data => this.loader(data, this.outputFilename)));
		} else if (this.type === 'db') {
			this.observable$ = this.observable$.pipe(map(data => this.loader(data, this.connectionString, this.collectionName)));
		}
			
		return this;
	}

	/**
	 * Subscribes to the Observable, in Etl's state, encapsulating the entire Etl process
	 * 
	 * @returns {string} message - send back a message declaring success or failure
	 */
	start() {
		if (this.observable$ === null) 
			return console.error('Error: Failed to start. Please make sure extractors, transformers, loaders were added and combined using the .combine() method.\n');
		let message = '';
		// close the database connection upon completion, return error if error is thrown
		this.observable$.subscribe(	
			null, 
			(err) => console.error('Error: unable to start etl process.\n', err),
			null
		);
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
	simple(extractString, callback, loadString, collectionName = 'etl_output') {
		// validate input
		if (extractString === undefined || typeof extractString !== 'string' || extractString.length === 0) 
			return console.error('Error: first parameter of simple() must be a string and cannot be empty!');
		if (callback === undefined || typeof callback !== 'function') 
			return console.error('Error: second parameter of simple() must be a function and cannot be empty!');
		if (loadString === undefined || typeof loadString !== 'string' || loadString.length === 0) 
			return console.error('Error: third parameter of simple() must be a string and cannot be empty!');

		// add valid callbacks to the list of transformers in state
		this.transformers.push(callback);

		/* EXTRACT: check extractString to choose appropriate extractor */
		if (fileExtension(extractString).toLowerCase() === 'csv') this.extractor$ = extract.fromCSV(extractString);
		if (fileExtension(extractString).toLowerCase() === 'json') this.extractor$ = extract.fromJSON(extractString);
		if (fileExtension(extractString).toLowerCase() === 'xml') this.extractor$ = extract.fromXML(extractString);

		// buffer the observable to collect 99 at a time
		this.extractor$ = this.extractor$.pipe(bufferCount(1000, 1000));

		// LOAD: check loadString to load to appropriate database
		if (fileExtension(loadString).toLowerCase() === 'csv') this.loader = load.toCSV;
		if (fileExtension(loadString).toLowerCase() === 'json') this.loader = load.toJSON;
	  if(fileExtension(loadString).toLowerCase() === 'xml') this.loader = load.toXML;
		if (connectionString(loadString).protocol && connectionString(loadString).protocol === 'postgres') {
			this.loader = load.toPostgres;
			this.connectionString = loadString;
			this.collectionName = collectionName;
		}
	  if (connectionString(loadString).protocol && connectionString(loadString).protocol === 'mongodb') {
			this.loader = load.toMongoDB;
			this.connectionString = loadString;
			this.collectionName = collectionName;
		}
		return this;
	}
}

module.exports = Etl;