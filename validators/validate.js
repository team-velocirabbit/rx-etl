const { Observable } = require('rxjs');
const connectionString = require('connection-string');
const fileExtension = require('file-extension');
const extract = require('../extractors/extract');
const load = require('../loaders/load');

// An object storing all error handlers / validators
const validate = {};

/**
 * Validates args passed in to addExtractors method of Etl.js
 * and resets Etl's state whenever error is encountered
 * 
 * @param {object} etl - Etl object
 * @param {string} type - string signifying type of extract source (flat file/db)
 * @param {string} dbType - string signifying mongodb/postgres from db connection string
 * @param {string} fileExt - string signifying csv/json/xml from file path
 */
validate.extractorArgs = (etl, type, dbType, fileExt) => {
  if (((type === 'csv' || type === 'xml' || type === 'json') && (type !== fileExt)) 
    || ((type === 'mongodb' || type === 'postgres') && (type !== dbType))) {
    etl.reset();
    throw new Error('please make sure extract function matches file type! \n');
  }
};

/**
 * Checks if extractor$ is an Observable
 * and resets Etl's state whenever error is encountered
 * 
 * @param {object} etl - Etl object
 * @param {Observable} extractor$ - Observable encapsulating extraction process
 */
validate.extractor$ = (etl, extractor$) => {
  if (!(extractor$ instanceof Observable)) {
    etl.reset();
    throw new Error('extractor function invalid\n See docs for more details.\n');
  } else etl.extractor$ = extractor$;
};

/**
 * Checks if transfomer is a function
 * and resets Etl's state whenever error is encountered
 * 
 * @param {object} etl - Etl object
 * @param {function} transformer - function to transforms streams of data
 * @returns {boolean} - identifies whether passed in transformer is a function 
 */
validate.transformer = (etl, transformer) => {
  if (!(transformer instanceof Function)) {
    etl.reset();
    throw new Error('argument must be an array of functions!\n See docs for more details.\n');
  }
  return true;
};

/**
 * Checks if loader is a function
 * and resets Etl's state whenever error is encountered
 * 
 * @param {object} etl - Etl object
 * @param {function} loader - function to load streams of data to an output source
 * @returns {boolean} - identifies whether passed in loader is a function 
 */
validate.loader = (etl, loader) => {
  if (!(loader instanceof Function)) {
    etl.reset();
    throw new Error("loader functions must be of class 'Loaders'\n See docs for more details.\n");
  }
  return true;
};

/**
 * Validates args passed into addLoaders method of Etl.js
 * and resets Etl's state whenever error is encountered
 * 
 * @param {object} etl - Etl object
 * @param {string} type - string signifying type of extract source (flat file/db)
 * @param {string} connectStrOrFilePath - connection string or file path to export to
 * @param {string} collectionNameOrFileName - collection name or file name to export to 
 */
validate.loaderArgs = (etl, type, connectStrOrFilePath, collectionNameOrFileName) => {
  // makes sure that connectionString and collectionName are provided if loader is to a database
  if ((type === 'mongodb' || type === 'postgres')
    && ((typeof connectStrOrFilePath !== 'string' || connectStrOrFilePath.length === 0)
    || (typeof collectionNameOrFileName !== 'string' || collectionNameOrFileName.length === 0))) {
    etl.reset();
    throw new Error('database loaders must provide connection string AND collection / table name in the parameter!\n');
  } 
  // make sure filename is provided if loading to flat file
  if ((type === 'csv' || type === 'xml' || type === 'json')
    && ((connectStrOrFilePath && typeof connectStrOrFilePath !== 'string')
    || (collectionNameOrFileName && typeof collectionNameOrFileName !== 'string'))) {
    etl.reset();
    throw new Error('flatfile loaders must provide valid output file path and/or file name!\n');
  }
};

/**
 * Validates collection name or file name to match the type of output source
 * 
 * @param {string} type - string signifying type of extract source (flat file/db)
 * @param {string} collectionNameOrFileName - collection name or file name to export to 
 */
validate.loaderOutputFile = (type, collectionNameOrFileName) => {
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
};

/**
 * Checks to make sure no previous Etl process exists before proceeding with current process
 * 
 * @param {object} etl - Etl object
 */
validate.newEtlProcess = etl => {
  if (etl.observable$ !== null) throw new Error('Failed to combine. Please make sure previous ETL process does not exist and try using the .reset() method\n');
};

/**
 * Validates args passed in to start method of Etl.js
 * and Etl Observable has been created before starting process
 * 
 * @param {object} etl - Etl object
 * @param {boolean} arg - argument passed into the start method
 */
validate.beforeStart = (etl, arg) => {
  // verify arg passed in to .start() method
  if (typeof arg !== 'boolean') {
    throw new Error("invalid arg to .start() method! Please make sure arg is type 'boolean'!\n");
  }
  // check to make sure user invoked .combine() before invoking start method
  if (etl.observable$ === null) {
    throw new Error('Failed to start. Please make sure extractors, transformers, loaders were added and combined using the .combine() method.\n');
  }
};

/**
 * Validates args passed in to simple method of Etl.js
 * 
 * @param {string} extractString - name of source to extract from (db connection str or file path)
 * @param {string} extractCollection - name of source collection/table to extract from
 * @param {array} callback - array of transform functions
 * @param {string} connectStrOrFilePath - connection string or file path to export to
 * @param {string} collectionNameOrFileName - collection name or file name to export to
 */
validate.simpleArgs = (extractString, extractCollection, callback, connectStrOrFilePath, collectionNameOrFileName) => {
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
};

/**
 * Checks extractString to store appropriate extract method in the etl object
 * 
 * @param {object} etl - Etl object
 * @param {string} extractString - name of source to extract from (db connection str or file path)
 * @param {string} extractCollection - name of source collection/table to extract from
 * @param {array} callback - array of transform functions
 * @param {string} connectStrOrFilePath - connection string or file path to export to
 * @param {string} collectionNameOrFileName - collection name or file name to export to
 */
validate.simpleExtract = (etl, extractString, extractCollection, collectionNameOrFileName) => {
  if (fileExtension(extractString).toLowerCase() === 'csv') etl.extractor$ = extract.fromCSV(extractString);
  if (fileExtension(extractString).toLowerCase() === 'json') etl.extractor$ = extract.fromJSON(extractString);
  if (fileExtension(extractString).toLowerCase() === 'xml') etl.extractor$ = extract.fromXML(extractString);
  if (((connectionString(extractString).protocol && connectionString(extractString).protocol === 'mongodb')
    || (connectionString(extractString).protocol && connectionString(extractString).protocol === 'postgres'))
    && !extractCollection) {
    throw new Error('extracting from database requires table (postgres) / collection (mongodb) name.');
  }
  if (connectionString(extractString).protocol && connectionString(extractString).protocol === 'mongodb') {
    etl.extractor$ = extract.fromMongoDB(extractString, extractCollection);
  }
  if (connectionString(extractString).protocol && connectionString(extractString).protocol === 'postgres') {
    etl.extractor$ = extract.fromPostgres(extractString, collectionNameOrFileName);
  }
};

/**
 * Checks collectionNameOrFileName to store appropriate load method 
 * along with output file name, path, and type in the etl object
 * 
 * @param {object} etl - Etl object
 * @param {string} connectStrOrFilePath - connection string or file path to export to
 * @param {string} collectionNameOrFileName - collection name or file name to export to
 */
validate.simpleLoad = (etl, connectStrOrFilePath, collectionNameOrFileName) => {
  if (fileExtension(collectionNameOrFileName).toLowerCase() === 'csv') {
    etl.loader = load.toCSV;
    etl.outputFilePath = connectStrOrFilePath;
    etl.outputFileName = collectionNameOrFileName;
    etl.type = 'flatfile';
  } else if (fileExtension(collectionNameOrFileName).toLowerCase() === 'json') {
    etl.loader = load.toJSON;
    etl.outputFilePath = connectStrOrFilePath;
    etl.outputFileName = collectionNameOrFileName;
    etl.type = 'flatfile';
  } else if (fileExtension(collectionNameOrFileName).toLowerCase() === 'xml') {
    etl.loader = load.toXML;
    etl.outputFilePath = connectStrOrFilePath;
    etl.outputFileName = collectionNameOrFileName;
    etl.type = 'flatfile';
  } else if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'postgres') {
    etl.loader = load.toPostgres;
    etl.connectionString = connectStrOrFilePath;
    etl.collectionName = collectionNameOrFileName;
    etl.type = 'db';
  } else if (connectionString(connectStrOrFilePath).protocol && connectionString(connectStrOrFilePath).protocol === 'mongodb') {
    etl.loader = load.toMongoDB;
    etl.connectionString = connectStrOrFilePath;
    etl.collectionName = collectionNameOrFileName;
    etl.type = 'db';
  } else throw new Error('invalid load file name / collection name given. Please make sure to add the extension to the new file name.');
};

/**
 * Validates cron string format
 * 
 * @param {string} cron - schedule in cron-format to schedule job
 */
validate.cron = cron => {
  let parsedCron = cron.split(' ');
  if (parsedCron.length < 5 || parsedCron.length < 6) throw new Error('cron format is invalid! \n');
  parsedCron = parsedCron.join(' ').replace(/[*0-9]/g, '').trim();
  if (parsedCron.length !== 0) throw new Error('cron format is invalid. \n');
};

module.exports = validate;
