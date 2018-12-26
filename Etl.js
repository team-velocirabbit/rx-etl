const { Observable } = require('rxjs');
const { switchMap } = require('rxjs/operators');

class Etl {
	constructor() {
		this.extractor$ = null;
		this.transformers = [];
		this.loader = null;
		this.observable$ = null;
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
			if (!(transformers[i] instanceof Transformers)) {
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
	addLoaders(loader) {
		// make sure that the loaders passed in are of class Loaders
		if (!(loader instanceof Loaders)) {
			// reset Etl's state
			this.reset();
			return console.error("Error: loader functions must be of class 'Loaders'\n See docs for more details.\n")
		} else {
			// push loader function to this.loaders
			this.loader = loader;
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
		this.observable$ = this.observable$.pipe(map(data => this.loader(data)));

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
}

module.exports = Etl;