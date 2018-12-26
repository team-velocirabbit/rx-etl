const { Observable } = require('rxjs');
const { switchMap, map } = require('rxjs/operators');
const fileExtension = require('file-extension');
const connectionString = require('connection-string');

const extract = require('./extractors/extract');
const load = require('./loaders/load');

class Etl {
	constructor() {
		this.extractor$ = null;
		this.transformers = [];
		this.loader = null;
		this.observable$ = null;
		this.connectionString = '';
		this.collectionName = '';
	}

	/** 
   * Collects extractors
	 * 
	 * Stores, inside of the Etl object, the extractor observable that is passed in
	 * 
	 * @param {Observable} extractor$ observable that extracts data from source
	 * @return {Etl} return the object itself
	 */
	addExtractors(extractor$) {
		// make sure that the extractor passed in is instance of Observable
		if (!(extractor$ instanceof Observable)) {
			// reset Etl's state
			this.reset();
			return console.error("Error: extractor function invalid\n See docs for more details.\n");
		} else {
			// save extractor$ to ETL's state
			this.extractor$ = extractor$;
		}
		return this;
	}


	/**********	method to add transformers **********/
	addTransformers(...transformers) {
		// make sure that the transformers passed in are instances of Transformers
		for (let i = 0; i < transformers.length; i += 1) {
			if (!(transformers[i] instanceof Function)) {
				// reset Etl's state
				this.reset();
				return console.error("Error: transformer functions must be of class 'Transformers'\n See docs for more details.\n")
			} else {
				// push transformer to state
				this.transformers.push(transformers[i]);
			}
		}
		return this;
	}


	/********** method to add loaders **********/
	addLoaders(loader, connectionString, collectionName) {
		// make sure that the loaders passed in are of class Loaders
		if (!(loader instanceof Function)) {
			// reset Etl's state
			this.reset();
			return console.error("Error: loader functions must be of class 'Loaders'\n See docs for more details.\n")
		} else {
			// push loader function to this.loaders
			this.loader = loader;
			this.connectionString = connectionString;
			this.collectionName = collectionName;
		}
		return this;
	}


	/********** method to wrap in observer **********/
	combine() {
		// if previous etl process exists, throw error
		if (this.observable$ !== null) 
			return console.error('Error: Failed to combine. Please make sure previous ETL process does not exist and try using the .reset() method\n')

		// create a new observable from the extractor$ observable, and complete the extractor$ observable
		this.observable$ = this.extractor$.pipe(switchMap(data => Observable.create(observer => {
			observer.next(data);
		})));

		// pipe the observable with transformers
		for (let i = 0; i < this.transformers.length; i += 1) {
			this.observable$ = this.observable$.pipe(map(data => this.transformers[i](data)));
		}

		// pipe the observable to the loader function
		this.observable$ = this.observable$.pipe(map(data => this.loader(data, this.connectionString, this.collectionName)));

		return this;
	}


	/********** method to invoke the observer **********/
	start() {
		// checks to make sure everything has been combined before starting
		if (this.observable$ === null) 
			return console.error('Error: Failed to start. Please make sure extractors, transformers, loaders were added and combined using the .combine() method.\n')
		return this.observable$.subscribe();
	}


	/********** method to clear state in case of error **********/
	reset() {
		this.extractor$ = null;
		this.transformers = [];
		this.loader = null;
		this.observable$ = null;
		return this;
	}



	/* simple shortcut method for GUI interaction */
	simple(extractString, callback, loadString, collectionName = 'etl_output') {

		// check for valid input
		if (extractString === undefined || typeof extractString !== 'string' || extractString.length === 0) 
			return console.error('Error: first parameter of simple() must be a string and cannot be empty!')
		if (callback === undefined || typeof callback !== 'function') 
			return console.error('Error: second parameter of simple() must be a function and cannot be empty!')
		if (loadString === undefined || typeof loadString !== 'string' || loadString.length === 0) 
			return console.error('Error: third parameter of simple() must be a string and cannot be empty!')

		// add callback to state
		this.transformers.push(callback);

		/* EXTRACT: check extractString to choose appropriate extractor */
		if (fileExtension(extractString).toLowerCase() === 'csv') this.extractor$ = extract.fromCSV(extractString);
		if (fileExtension(extractString).toLowerCase() === 'json') this.extractor$ = extract.fromJSON(extractString);
		if (fileExtension(extractString).toLowerCase() === 'xml') this.extractor$ = extract.fromXML(extractString);

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