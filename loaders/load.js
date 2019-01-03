const { Observable } = require('rxjs');
const { MongoClient } = require('mongodb');
const fs = require('file-system');
var csvWriter = require('csv-write-stream')

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

load.toJSON = () => {

};

load.toXML = () => {

};

/**
 * Exports transformed data to a Mongo database
 * 
 * @param {object} - data to export to file/db
 * @param {dbClient} - connection to MongoDB collection
 * @return
 */

load.toMongoDB = (data, newCollection) => { // Do we need to add a collection name field to the UI?
  // Inserting a new row into the Mongo collection
  newCollection.insert(data);
  return;
};

load.toPostgres = () => {

};

module.exports = load;
