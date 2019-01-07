const { Observable } = require('rxjs');
const fileExtension = require('file-extension');
const csv = require('csv-parser');
const fs = require('file-system');
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
  if (!fileExtension(filePath).toLowerCase() === 'csv') throw new Error('File does not appear to be CSV.\n');

  // Return an observable containing the CSV data
  return Observable.create((observer) => {
    const data = fs.createReadStream(filePath).pipe(csv());
    data.on('data', chunk => observer.next(chunk));
    data.on('end', () => observer.complete());
    data.on('error', () => { throw new Error('Error: there was an error reading the extract file.') });

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
  if (filePath === undefined) throw new Error('A file path does not appear to have been passed.\n');

  // Check if the file extension is JSON
  if (!fileExtension(filePath).toLowerCase() === 'json') throw new Error('File does not appear to be JSON.\n');

  // Return an observable containing the XML data
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
  if (filePath === undefined) throw new Error('A file path does not appear to have been passed.\n');
 
  // Check if the file extension is XML
  if (!fileExtension(filePath).toLowerCase() === 'xml') throw new Error('File does not appear to be JSON.\n');

  return Observable.create((observer) => {
    

    // good luck


  });
};

/**
 * Description
 *
 * @param {} - 
 * @return {} - 
 */
extract.fromMongoDB = () => {

}

/**
 * Description
 *
 * @param {} - 
 * @return {} - 
 */
extract.fromPostgres = () => {
  
}

module.exports = extract;
