const { MongoClient } = require('mongodb');
const fs = require('file-system');
const fileExtension = require('file-extension');
const csvWriter = require('csv-write-stream');
const path = require('path');

const load = {};

/**
 * Exports transformed data locally to a CSV file
 * 
 * @param {object} data - An array of rows to export to file/db
 * @param {string} filePath - A file path for the exported CSV file
 * @param {string} fileName - A file path and name for the exported CSV file
 * @return {string}
 */
load.toCSV = (data, filePath, fileName) => {
  // Check if data parameter is empty
  if (data.length === 0) return console.err('Error: No data was passed into the load method! \n');
  const outputFile = filePath + '/' + fileName;
  const writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFile, {'flags': 'a'}));
  data.forEach(record => writer.write(record, (data, err) => {
    if (err) return console.error('Error: there was an error writing data to output CSV file! \n');
  }))
  writer.end();
  return ;
};

/**
 * Exports transformed data locally to a JSON file
 * 
 * @param {array} data - An array of objects containing the data to be exported
 * @param {string} filePath - A file path for the exported JSON file
 * @param {string} fileName - A file name for the exported JSON file
 * @return
 */
load.toJSON = (data, filePath, fileName) => {
  // Check if data paramenter is empty
  if (data.length === 0) return console.error('Error: No data was passed into the load method! \n');
  const outputFile = filePath + '/' + fileName;
  // Check if the file extension is JSON
  if (!fileExtension(fileName).toLowerCase() === 'json') return console.error('ERROR: File does not appear to be JSON.\n');
  fs.appendFile(outputFile, JSON.stringify(data, null, '\t'), (err) => {
    if (err) return console.error('Error: There was a issue writing data to the JSON file. ', err);
  });
  return;
};

load.toXML = () => {

};

/**
 * Exports transformed data to a Mongo database
 * 
 * @param {object} - data to export to file/db
 * @param {string} - connection string to the Mongo database
 * @param {string} - name of the desired collection
 * @return
 */


// One row at a time
load.toMongoDB = (data, connectionString, collectionName) => { // Do we need to add a collection name field to the UI?
  // Setting up and connecting to MongoDB
  MongoClient.connect(connectionString, (err, db) => {
    // Handling connection errors
    if (err) return console.error(err);
    // Creating a new collection in the Mongo database
    let bulk = db.collection(collectionName).initializeOrderedBulkOp();
    // insert each data row into bulk
    data.forEach(d => bulk.insert(d));
    // bulk insert to database
    bulk.execute();
  });
  return;
};

load.toPostgres = () => {

};

module.exports = load;
