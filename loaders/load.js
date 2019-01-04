const { Observable } = require('rxjs');
const { MongoClient } = require('mongodb');
const fs = require('file-system');
var csvWriter = require('csv-write-stream')

const load = {};

/**
 * Exports transformed data locally to a CSV file
 * 
 * @param {object} data - array of rows to export to file/db
 * @param {string} outputFile - a file path and name for the exported CSV file
 * @return
 */
load.toCSV = (data, outputFile) => {
  const writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFile + '.csv', {'flags': 'a'}));
  data.forEach(record => writer.write(record))
  writer.end();
  return;
};

load.toJSON = () => {

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
