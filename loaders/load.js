const { MongoClient } = require('mongodb');
const fs = require('file-system');
const fileExtension = require('file-extension');
const csvWriter = require('csv-write-stream');
const js2xmlparser = require("js2xmlparser");
const path = require('path');

const load = {};

/**
 * Exports transformed data locally to a CSV file
 * 
 * @param {object} data - An array of rows to export to file/db
 * @param {string} filePath - A file path for the exported CSV file
 * @param {string} fileName - A file path and name for the exported CSV file
 * @param {integer} initialWrite - A counter that tracks number of times file is written to
 * @return
 */
load.toCSV = (data, filePath, fileName, initialWrite) => {
  // Check if data parameter is empty
  console.log(initialWrite)
  if (data.length === 0) return console.err('Error: No data was passed into the load method! \n');
  const outputFile = filePath + '/' + fileName;
  const writer = csvWriter();
  if (initialWrite === 0) writer.pipe(fs.createWriteStream(outputFile));
  else writer.pipe(fs.createWriteStream(outputFile, {'flags': 'a'}));
  data.forEach(record => writer.write(record, (data, err) => {
    if (err) return console.error('Error: there was an error writing data to output CSV file! \n');
  }))
  writer.end();
  return;
};

/**
 * Exports transformed data locally to a JSON file
 * 
 * @param {array} data - An array of objects containing the data to be exported
 * @param {string} filePath - A file path for the exported JSON file
 * @param {string} fileName - A file name for the exported JSON file
 * @param {integer} initialWrite - A counter that tracks number of times file is written to
 * @return
 */
load.toJSON = (data, filePath, fileName, initialWrite) => {
  // Check if data paramenter is empty
  if (data.length === 0) return console.error('Error: No data was passed into the load method! \n');
  const outputFile = filePath + '/' + fileName;
  // Check if the file extension is JSON
  if (!fileExtension(fileName).toLowerCase() === 'json') return console.error('ERROR: File does not appear to be JSON.\n');
  if (initialWrite === 0) fs.writeFile(outputFile, JSON.stringify(data, null, '\t'), (err) => {
    if (err) return console.error('Error: There was an issue writing data to the JSON file. ', err);
  });
  else 
  fs.appendFile(outputFile, JSON.stringify(data, null, '\t'), (err) => {
    if (err) return console.error('Error: There was a issue writing data to the JSON file. ', err);
  });
  return;
};

/**
 * Exports transformed data locally to an XML file
 *
 * @param {array} data - An array of objects containing the data to be exported
 * @param {string} filePath - A file path for the exported XML file
 * @param {string} fileName - A file name for the exported XML file
 * @param {integer} initialWrite - A counter that tracks number of times file is written to
 * @return
 */
load.toXML = (data, filePath, fileName, initialWrite) => {
  // Check if data paramenter is empty
  if (data.length === 0) return console.error('Error: No data was passed into the load method! \n');
  // Check if the file extension is XML
  if (!fileExtension(fileName).toLowerCase() === 'xml') return console.error('ERROR: File does not appear to be XML.\n');
  const xmlData = js2xmlparser.parse("dataset", data);
  const outputFile = filePath + '/' + fileName;
  if (initialWrite === 0) fs.writeFile(outputFile, xmlData, (err) => {
    if (err) return console.error('Error: There was an issue writing data to the XML file. ', err)
  });
  else fs.appendFile(outputFile, xmlData, (err) => {
    if (err) return console.error('Error: There was an issue writing data to the XML file. ', err);
  });
  return;
};

/**
 * Exports transformed data to a Mongo database
 * 
 * @param {array} data - An array of objects containing the data to be exported
 * @param {string} connectionString - connection string to the Mongo database
 * @param {string} collectionName - name of the desired collection
 * @return
 */
load.toMongoDB = (data, connectionString, collectionName) => {
  // Setting up and connecting to MongoDB
  MongoClient.connect(connectionString, (err, db) => {
    // Handling connection errors
    if (err) return console.error(err);
    // Creating a new collection in the Mongo database
    const bulk = db.collection(collectionName).initializeOrderedBulkOp();
    // insert each data row into bulk
    data.forEach(d => bulk.insert(d));
    // bulk insert to database
    bulk.execute();
  });
  return;
};

/**
 * Exports transformed data to a Postgres database
 * 
 * @param {array} data - An array of objects containing the data to be exported
 * @param {string}
 * @param {string}
 * @return
 */
load.toPostgres = (data) => {
  // Insert code here
};

module.exports = load;
