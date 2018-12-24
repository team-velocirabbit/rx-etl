const { Observable } = require('rxjs');

const fileExtension = require('file-extension');
const csv = require('csv-parser');

const path = require('path');
const fs = require('file-system');

// An object containing all the extract methods
const extract = {};

/** 
* SUMMARY: This method imports a CSV file from the file system using file path parameter and processes the file
* @param: {String} A path to the input file
* @return: {Observable} An observable containing the parsed CSV data
*/
extract.fromCSV = (filePath) => {
  // Check if a file path was passed into the function
  if (filePath === undefined) return console.error('ERROR: A file path does not appear to have been passed.\n')
  
  // Check if the file extension is CSV
  if (!fileExtension(filePath).toLowerCase() === 'csv') return console.error('ERROR: File does not appear to be a CSV.\n') 
  
  // Return an observable containing the CSV data
  return Observable.create(observer => {
    let data = fs.createReadStream(filePath).pipe(csv());
  
    data.on('data', chunk => observer.next(chunk));
    data.on('end', () => observer.complete());

    // Closing the stream 
    return () => data.pause();
  });
};

extract.fromJSON = () => {

};

extract.fromXML = () => {

};

module.exports = extract;