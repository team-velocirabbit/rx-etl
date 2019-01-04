const { MongoClient } = require('mongodb');
const fs = require('file-system');
const fileExtension = require('file-extension');
const csvWriter = require('csv-write-stream');

const load = {};

/**
 * Exports transformed data locally to a CSV file
 * 
 * @param {object} - data to export to file/db
 * @param {string} - a file path and name for the exported CSV file
 * @return
 */
load.toCSV = (row, outputFile) => {
  const writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFile));
  writer.write(row);
  writer.end();
  return;
};

/**
 * Exports transformed data locally to a JSON file
 * 
 * @param {array} - An array of objects containing the data to be exported
 * @param {string} - A file path for the exported CSV file
 * @param {string} - A file name for the exported CSV file
 * @return {string} - A console log indicating success or error
 */
load.toJSON = (data, filePath, fileName) => {
  // Check if data paramenter is empty
  if (data.length === 0) return console.error('Error: No data was passed into this method');
  // Check if file path is empty or missing
  if (!filePath) return console.error('Error: A file path was not passed into this method');
  // Check if file name is empty or missing
  if (!fileName) return console.error('Error: A file name was not passed into this method');
  // Check if the file extension is JSON
  if (!fileExtension(fileName).toLowerCase() === 'json') return console.error('ERROR: File does not appear to be JSON.\n');
  fs.appendFile(`${filePath}/${fileName}`, JSON.stringify(data, null, '\t'), (err) => {
    if (err) console.error('Error: There was a issue writing data to the JSON file. ', err);
    else console.log('Success: The JSON files was successfully created.');
  });
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
load.toMongoDB = (data, connectionString, collectionName, message) => { // Do we need to add a collection name field to the UI?
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
