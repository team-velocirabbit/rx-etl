const { Observable } = require('rxjs');
const { MongoClient } = require('mongodb');
const fs = require('file-system');
var csvWriter = require('csv-write-stream')

const load = {};

/**
* SUMMARY: This method exports transformed data locally to a CSV file
* @param: { }
  @param: {String} A file path and name for the exported file
* @return: { }
*/
load.toCSV = (row, outputFile) => {
  const writer = csvWriter();
  writer.pipe(fs.createWriteStream(outputFile));
  writer.write(row);
  writer.end();
};

load.toJSON = () => {

};

load.toXML = () => {

};

/**
* SUMMARY: This method exports transformed data to a Mongo database
* @param: { }
  @param: {String} A connection string to the Mongo database
  @param: {String} The name of the desired collection
* @return: { }
*/
load.toMongoDB = (row, connectionString, collectionName) => { // Do we need to add a collection name field to the UI?
  // Setting up and connecting to MongoDB
  MongoClient.connect(connectionString, (err, db) => {
    // Handling connection errors
    if (err) console.log(err);
    // Handling a successful database connection
    else console.log('Connected successfully to server');
    // Creating a new collection in the Mongo database
    const newCollection = db.collection(collectionName);
    // Inserting a new row into the Mongo collection
    newCollection.insertOne(row);
    // Close the Mongo connection
    db.close();
  });
};

load.toPostgres = () => {

};

module.exports = load;
