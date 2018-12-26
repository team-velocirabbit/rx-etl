const { Observable } = require('rxjs');
const { MongoClient } = require('mongodb');

const load = {};

load.toCSV = () => {

};

load.toJSON = () => {
    
};

load.toXML = () => {
    
};

/**
* SUMMARY: This method imports a CSV file from the file system using
* file path parameter and processes the file
* @param: 
  @param:
  @param:
* @return: 
*/

// connectionString, collectionName 

load.toMongoDB = (row, connectionString, collectionName) => { // Do we need to add a collection name field to the UI?

  // Setting up and connecting to MongoDB
  MongoClient.connect(connectionString, (err, db) => {
    // Handling connection errors
    if (err) console.error(err);
    // Handling a successful database connection
    // else console.log('Connected successfully to server');
    
    // Creating a new collection in the Mongo database
    const newCollection = db.collection(collectionName);
    // Inserting a new row into the Mongo collection
    newCollection.insertOne(row);
    // Close the Mongo connection
    // db.close();
  });
  
  return;
};

load.toPostgres = () => {

};

module.exports = load;
