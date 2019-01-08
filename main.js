const { Observable, Subject, of, from, fromEvent } = require('rxjs');
const { create, concat, map, takeUntil } = require('rxjs/operators');
const readline = require('readline');
const scheduler = require('node-schedule');

// TESTING LIBRARY 123123123123 
const testEtl = require('./Etl');
const extract = require('./extractors/extract');
const transform = require('./transformers/transform');
const load = require('./loaders/load');

//******************** */
 
const JSONStream = require('JSONStream');


const csv = require('csv-parser');

const express = require('express');
const path = require('path');
const fs = require('file-system');
const etl = require('etl');
const mongodb = require('mongodb');


const pg = require('pg');
const copyFrom = require('pg-copy-streams').from;

let pgClient = new pg.Client('postgres://pssshksz:Wh0grf6b-steQ88Dl0EIqk06siRpayld@pellefant.db.elephantsql.com:5432/pssshksz?ssl=true')
pgClient.connect();

const MongoClient = mongodb.MongoClient;
const Collection = mongodb.Collection;

let collection;
let csvCollection;
let jsonCollection;

// establish mongodb connection
MongoClient.connect('mongodb://dbadmin:admin1234@ds157549.mlab.com:57549/npm-etl-test', (err, db) => {
	csvCollection = db.collection("csvCollection");
	jsonCollection = db.collection("jsonCollection");
})

const app = express();
const PORT = 3000;

const chooseMockFile = (req, res, next) => {
	res.locals.filename = 'MOCK_DATA.csv';
	res.locals.type = 'csv';
	collection = csvCollection;
	return next();
};

const chooseMockFilePg = (req, res, next) => {
	res.locals.filename = 'MOCK_DATA.csv';
	res.locals.type = 'csv';
	return next();
};

const chooseTestFile = (req, res, next) => {
	res.locals.filename = 'test.csv';
	return next();
};

const extractCsv = (sourceType, file) => {
	return Observable.create(observer => {
		let file$; 
		if (sourceType === 'csv') file$ = fs.createReadStream(file).pipe(csv());
		if (sourceType === 'json') file$ = file;

		file$.on('data', chunk => observer.next(chunk));
		file$.on('end', () => observer.complete());

		// close the stream 
		return () => file$.pause();
	});
};

// returns an observable
const transformObservable = (fileReader$, ...transformFunc) => {
	for (let i = 0; i < transformFunc.length; i += 1) {
		fileReader$ = fileReader$.pipe(map(data => transformFunc[i](data)));
	}
	return fileReader$;
};

const storeInMongo = (data) => {
	return collection.insertOne(data);
};

const storeInPg = (data) => {
	// const query = 'INSERT INTO test ("full_name", "email_address", "password", "phone", "street_address", "city", "postal_code", "country") VALUES ($1, $2, $3, $4, $5, $6, $7, $8)';
	// const values = [data['full_name'], data['email_address'], data['password'], data['phone'], data['street_address'], data['city'], data['postal_code'], data['country']];
	// return pgClient.query(query, values);
	return pgClient.query(copyFrom('COPY test (id, first_name, last_name, email_address, password, phone, street_address, city, postal_code, country) FROM STDIN CSV HEADER'));
};

// returns changed entry
const combineNames = (data) => {
	const nd = {};
	nd.id = data.id * 1;
	nd.full_name = data["first_name"] + ' ' + data["last_name"];
	nd.email_address = data.email_address;
	nd.password = data.password;
	nd.phone = data.phone.replace(/[^0-9]/g, ''); 
	nd.street_address = data.street_address;
	nd.city = data.city;
	nd.postal_code = data.postal_code;
	nd.country = data.country;
	nd["__line"] = (data.id * 1) + 1;
	return nd;
};

const jsonToCsv = (req, res, next) => {
	res.locals.filename = fs.createReadStream('MOCK_DATA.json', { flags: 'r', encoding: 'utf-8' }).pipe(JSONStream.parse('*'));
	res.locals.type = 'json';	
	collection = jsonCollection;
	return next();
};

const csvToMongo = async (req, res, next) => {
	const fileReader$ = extractCsv(res.locals.type, res.locals.filename);
	res.locals.data = transformObservable(fileReader$, combineNames, storeInMongo);
	return next();
};

const csvToPg = (req, res, next) => {
	const fileReader$ = extractCsv(res.locals.type, res.locals.filename);
	res.locals.data = transformObservable(fileReader$, combineNames).pipe(storeInPg);
	return next();
};

app.get('/csvToMongo', chooseMockFile, csvToMongo, (req, res) => {
	res.locals.data.subscribe();
	res.sendStatus(200);
});

app.get('/jsonToMongo', jsonToCsv, csvToMongo, (req, res) => {
	res.locals.data.subscribe();
	res.sendStatus(200);
});

app.get('/csvToPg', chooseMockFilePg, csvToPg, (req, res) => {
	res.locals.data.subscribe();
	res.sendStatus(200);
});

app.get('/etlPg', (req, res) => {

	const stream = pgClient.query(copyFrom('COPY test (id, first_name, last_name, email_address, password, phone, street_address, city, postal_code, country) FROM STDIN CSV HEADER'));
	const fileStream = fs.createReadStream('test.csv');

	fileStream.pipe(stream);
	
	res.sendStatus(200);
});

app.get('/test', (req, res) => {

const filePath = '/Users/tkachler/Desktop';
const fileName = 'output.xml';
const emailMessage = {
	to: 'kachler@gmail.com',
	from: 'kachler@gmail.com',
	subject: 'RX-ETL job completed',
	text: 'Your RX-ETL job has finished.',
	html: '<strong>and easy to do anywhere, even with Node.js</strong>',
};

const textMessage = {
	to: '6193095463',
  body: 'Your RX-ETL job has finished.',
}

	new testEtl()
		.addExtractors(extract.fromCSV, 'MOCK_DATA_SHORT.csv')
 // .addExtractors(extract.fromMongoDB, 'mongodb://dbadmin:admin1234@ds157549.mlab.com:57549/npm-etl-test', 'pleasework')
		.addTransformers(combineNames)
		.addLoaders(load.toXML, 'josie.xml')
		.combine()		
		.addSchedule('1 * * * * *')																								
		.addEmailNotification(emailMessage)
		.addTextNotification(textMessage)
		.start()




		// Testing fromMongo => toXML
		// new testEtl()
	  // // .addExtractors(extract.fromCSV, '/Users/tkachler/Development/team-velocirabbit/rx-etl-1/MOCK_DATA.csv')
		// .addExtractors(extract.fromMongoDB, 'mongodb://dbadmin:admin1234@ds157549.mlab.com:57549/npm-etl-test', 'pleasework')
		// .addTransformers(combineNames)
		// .addLoaders(load.toXML, fileName, filePath)
		// // .addLoaders(load.toMongoDB, 'mongodb://dbadmin:admin1234@ds157549.mlab.com:57549/npm-etl-test', 'pleasework')
		// .combine()																										
		// .start()



	// const etl = new testEtl()
	// 	.simple('MOCK_DATA.csv', [combineNames], __dirname, 'pleasework.csv')
	// 	.combine()
	// 	.start()

	res.sendStatus(200);
});

app.listen(`${PORT}`, () => {
  console.log(`Server listening on PORT: ${PORT}`);
});