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
 * @param {string} - connection string to the Mongo database
 * @param {string} - name of the desired collection
 * @return
 */

load.toMongoDB = (data, connectionString, collectionName, message) => { // Do we need to add a collection name field to the UI?

  // Setting up and connecting to MongoDB
  MongoClient.connect(connectionString, (err, db) => {
    // Handling connection errors
    if (err) return console.error(err);
    
    // Creating a new collection in the Mongo database
    const newCollection = db.collection(collectionName);

    // Inserting a new row into the Mongo collection
    newCollection.insert(data);
  });
  
  return;
};

load.toPostgres = () => {

};

module.exports = load;
