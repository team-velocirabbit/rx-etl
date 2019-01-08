const { Observable } = require('rxjs');
const { MongoClient } = require('mongodb');
const pg = require('pg');
const QueryStream = require('pg-query-stream');
const fileExtension = require('file-extension');
const conStrParse = require('connection-string');
const csv = require('csv-parser');
const fs = require('file-system');
const now = require('performance-now');
const JSONStream = require('JSONStream');

// An object containing all the extract methods
const extract = {};

/**
 * Imports a CSV file from the file system using file path parameter and processes the file
 *
 * @param {string} - a path to the input file
 * @returns {Observable} - an observable containing the parsed CSV data
 */
extract.fromCSV = (filePath) => {
  // Check if a file path was passed into the function
  if (filePath === undefined) throw new Error('A file path does not appear to have been passed.\n');

  // Check if the file extension is CSV
  if (fileExtension(filePath).toLowerCase() !== 'csv') throw new Error('File does not appear to be CSV.\n');

  // Return an observable containing the CSV data
  return Observable.create((observer) => {
    const data = fs.createReadStream(filePath).pipe(csv());
    data.on('error', () => { throw new Error('Error: there was an error reading the extract file.'); });
    data.on('data', chunk => observer.next(chunk));
    data.on('end', () => observer.complete());

    // Closing the stream
    return () => data.pause();
  });
};

/**
 * Import a JSON file from the file system using file path parameter and processes the file
 *
 * @param {string} - a path to the input file
 * @return {Observable} - an observable containing the parsed JSON data
 */
extract.fromJSON = (filePath) => {
  // Check if a file path was passed into the function
  if (filePath === undefined) throw new Error('ERROR: A file path does not appear to have been passed.\n');

  // Check if the file extension is JSON
  if (fileExtension(filePath).toLowerCase() !== 'json') throw new Error('File does not appear to be JSON.\n');

  // Return an observable containing the JSON data
  return Observable.create((observer) => {
    const data = fs.createReadStream(filePath, { flags: 'r', encoding: 'utf-8' }).pipe(JSONStream.parse('*'));
    data.on('data', chunk => observer.next(chunk));
    data.on('end', () => observer.complete());
    // Closing the stream
    return () => data.pause();
  });
};

/**
 * Imports a XML file from the file system using file path parameter and processes the file
 *
 * @param {string} - a path to the input file
 * @return {Observable} - an observable containing the parsed XML data
 */
extract.fromXML = (filePath) => {
  // Check if a file path was passed into the function
  if (filePath === undefined) throw new Error('ERROR: A file path does not appear to have been passed.\n');

  // Check if the file extension is XML
  if (fileExtension(filePath).toLowerCase() !== 'xml') throw new Error('File does not appear to be XML.\n');

  return Observable.create((observer) => {
    // Add smart stuff here...
  });
};

/**
 * Import data from a Mongo collection
 *
 * @param {string} connectionString - connection string for the Mongo database
 * @param {string} collectionName - name of the desired collection
 * @return {Observable} - an observable containing the parsed Mongo collection data
 */
extract.fromMongoDB = (connectionString, collectionName) => {
  console.log('Starting fromMongoDB...');

  // Capturing start time for performance testing
  const start = now();

  // Setting batch size for Mongo query
  const bSize = 10000;

  // Check if a file path was passed into the function
  if (connectionString === '') throw new Error('You must provide a valid connection string!');

  // Check if the connection string uses the Mongo protocol
  const conStrObj = conStrParse(connectionString);
  if (conStrObj.protocol !== 'mongodb') throw new Error('Connection string does not appear to use the Mongo protocol!');

  // Check if a collection name was passed into the function
  if (collectionName === '') throw new Error('You must provide a valid collection name!');

  // Create a new observable
  return Observable.create((observer) => {
    // Connect to the database
    MongoClient.connect(connectionString, (err, db) => {
      // Handling errors connecting to the Mongo database
      if (err) return console.error(err);

      // Adding the collection name to the database connection
      const collection = db.collection(collectionName);

      // Find all the documents in the collection
      const stream = collection.find({}).batchSize(bSize).stream();

      // On error streaming data from Mongo...
      stream.on('error', () => { throw new Error('Error: there was an error streaming the Mongo data!'); });

      // On next streaming data from Mongo...
      stream.on('data', chunk => observer.next(chunk));

      // On completion of streaming data from Mongo...
      stream.on('end', () => {
        console.log('Mongo streaming completed...');
        console.log('Ending fromMongoDB...');

        // Captruing end time for performance testing
        const end = now();

        // Logging runtime
        console.log('Runtime:', msToTime(Math.abs(start - end)));

        // Closing database connection
        db.close();

        // Completing observer
        observer.complete();
      });

      // Closing the stream
      return () => stream.pause();
    });
  });
};

/**
 * Import data from a Postgres collection
 *
 * @param {string} connectionString - connection string for the Mongo database
 * @param {string} table - name of the desired table
 * @return {Observable} - an observable containing the parsed Postgres table data
 */
extract.fromPostgres = (connectionString, tableName) => {
  console.log('Starting fromPostgres...');

  // Capturing start time for performance testing
  const start = now();

  // Check if a file path was passed into the function
  if (connectionString === '') throw new Error('You must provide a valid connection string!');

  // Check if the connection string uses the Mongo protocol
  const conStrObj = conStrParse(connectionString);
  if (conStrObj.protocol !== 'postgres') throw new Error('Connection string does not appear to use the Postgres protocol!');

  // Check if a collection name was passed into the function
  if (tableName === '') throw new Error('You must provide a valid table name!');

  // Create a new observable
  return Observable.create((observer) => {

    const pgClient = new pg.Client(connectionString);
    // eslint-disable-next-line prefer-arrow-callback
    pgClient.connect(function (err, client) {
      // Handling errors connecting to the Postgres database
      if (err) throw new Error('Error: there was an error connecting to the Postgres database!');

      const query = new QueryStream('SELECT * FROM test');
      const stream = client.query(query);

      // On error streaming data from Postgres...
      stream.on('error', () => { throw new Error('Error: there was an error streaming the Postgres data!'); });

      // On next streaming data from Postgres...
      stream.on('data', chunk => observer.next(chunk));

      // On completion of streaming data from Postgres...
      stream.on('end', () => {
        console.log('Postgres streaming completed...');
        console.log('Ending fromPostgres...');

        // Captruing end time for performance testing
        const end = now();

        // Logging runtime
        console.log('Runtime:', msToTime(Math.abs(start - end)));

        // // Closing database connection
        pgClient.end();

        // Completing observer
        observer.complete();
      });
    });
  });
};

/**
 * A helper function for performance testing with the performance-now module
 * Converts milliseconds to hours, minutes, seconds and milliseconds
 *
 * @param {number} ms - A time in milliseconds
 * @return {string} - A string displaying hours, minutes, seconds and milliseconds
 */
function msToTime(ms) {
  const milliseconds = parseInt((ms%1000)/100);
  let seconds = parseInt((ms/1000)%60);
  let minutes = parseInt((ms/(1000*60))%60);
  let hours = parseInt((ms/(1000*60*60))%24);

  hours = (hours < 10) ? `0${hours}` : hours;
  minutes = (minutes < 10) ? `0${minutes}` : minutes;
  seconds = (seconds < 10) ? `0${seconds}` : seconds;

  return `${hours}:${minutes}:${seconds}.${milliseconds}`;
}

module.exports = extract;
