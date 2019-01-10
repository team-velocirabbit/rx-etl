# RxJS-ETL

RxJS-ETL is a modular platform utilizing RxJS observables to give developers the tools to build stream-based ETL (extract, transform, load) pipelines complete with buffering, bulk-insertions, notifications, and task dependencies.

##### Badges

[![NPM Version][npm-image]][npm-url]
[![Build Status][travis-image]][travis-url]
[![semantic-release][semantic-release-image]][semantic-release-url]
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

[npm-image]: https://img.shields.io/npm/v/rxjs-etl.svg
[npm-url]: https://npmjs.org/package/rxjs-etl 
[travis-image]: https://travis-ci.com/team-velocirabbit/rx-etl.svg?branch=master
[travis-url]: https://travis-ci.com/team-velocirabbit/rx-etl
[semantic-release-image]: https://img.shields.io/badge/%20%20%F0%9F%93%A6%F0%9F%9A%80-semantic--release-e10079.svg
[semantic-release-url]: https://github.com/semantic-release/semantic-release


## Installation

```
npm install rxjs-etl
```

## Usage
Require the RxJS-ETL library in the desired file to make is accessible

```js
const {Etl, extract, load} = require('rxjs-etl');
```

Sample configuration .csv -> mongodb
```js
	const task1 = new Etl()
		.addExtractors(extract.fromCSV, 'SAMPLE_DATA.csv')
		.addTransformers([transform1, transform2 ])
		.addLoaders(load.toMongoDB, mongoURI, collectionName)
		.combine()
		.addEmailNotification(email)
		.addTextNotification(text)
		.addSchedule('0 * * * * *')
		.next(task2)
		.start()
 ```

### Table of Contents
* [Extract](#extract)
* [Transform](#transform)
* [Load](#load)
* [Notifications](#notifications)
* [Scheduling](#scheduling)
* [Chaining](#chaining)
* [Misc](#misc)

### Extract
#### addExtractors(extractorFunction, connectStrOrFilePath, collectionOrTableName)
This method accepts one of the extractor helper methods as an argument and stores it to the Etl state. The second parameter is either the filepath or connection URI depending on the data source. The third paramenter is optional and consists of either the collection name or table name if extracting from a Mongo or Postgres database.

*The following helper methods are available for use with addExtractors:*
##### extract.fromCSV(filePath)
This method imports a CSV file from the file system, parses data into JSON and wraps the data in an observable. The method takes a file path as it's parameter.

Example
```js
.addExtractors(extract.fromCSV, '~/EXAMPLE_DATA.csv')
```

##### extract.fromJSON(filePath)
This method imports a JSON file from the file system, and wraps the data in an observable. The method takes a file path as it's parameter.

Example
```js
.addExtractors(extract.fromJSON, '~/EXAMPLE_DATA.json')
```

##### extract.fromXML(filePath)
This method imports a XML file from the file system, parses data into JSON and wraps the data in an observable. The method takes a file path as it's parameter.

Example
```js
.addExtractors(extract.fromXML, '~/EXAMPLE_DATA.xml')
```

##### extract.fromMongoDB(connectionString, collectionOrTableName)
This method connects to a Mongo database, imports the data from the desired collection, parses data into JSON and wraps the data in an observable. The method takes a connection URI and collection name as it's parameters.

Example
```js
.addExtractors(extract.fromMongoDB, process.env.MONGO_URI, collectionOrTableName)
```

##### extract.fromPostgres(connectionString, collectionOrTableName)
This method connects to a Postgres database, imports the data from the desired table,parses data into JSON and wraps the data in an observable. The method takes a connection URI and table name as it's parameters.

Example
```js
.addExtractors(extract.fromPostgres, process.env.POSTGRES_URI, collectionOrTableName)
```

---

### Transform
#### addTransformers(transformArray)
This method accepts an array of function supplied to the users. The addTransformers method will apply each function to the data stream.

---

### Load
#### addLoaders(loaderFunction, connectStrOrFilePath, collectionOrTableName)
This method accepts one of the loader helper methods as an arguments and stores it to the Etl state. The second parameter is either the filepath or connection URI depending on the data target. The third paramenter is optional and consists of either the collection name or table name if loading to a Mongo or Postgres database.

*The following helper methods are available for use with addLoaders:*
##### load.toCSV(filePath)
This method converts the transformed data to CSV an wrties the data to the file system. The method takes a file path (with file name included) as it's parameter.

Example
```js
.addExtractors(extract.toCSV, '~/EXAMPLE_DATA.csv')
```

##### load.toJSON(filePath)
This method converts the transformed data to JSON an wrties the data to the file system. The method takes a file path (with file name included) as it's parameter.

Example
```js
.addExtractors(extract.toJSON, '~/EXAMPLE_DATA.json')
```

##### load.toXML(filePath)
This method converts the transformed data to XML an wrties the data to the file system. The method takes a file path (with file name included) as it's parameter.

Example
```js
.addExtractors(extract.toXML, '~/EXAMPLE_DATA.xml')
```

##### load.toMongoDB(data,connectionString, collectionOrTableName)
This method connects to a Mongo database, converts the transformed data and streams it to the desired Mongo collection. The method takes the transformed data, a connection URI and collection name as it's parameters.

Example
```js
.addExtractors(load.toMongoDB, process.env.MONGO_URI, collectionOrTableName)
```

##### load.toPostgres(data,connectionString, collectionOrTableName)
This method connects to a Postgres database, converts the transformed data and streams it to the desired Postgres table. The method takes the transformed data, a connection URI and collection name as it's parameters.

Example
```js
.addExtractors(load.toPostgres, process.env.MONGO_URI, collectionOrTableName)
```

---

### Notifications
#### addEmailNotification(email)
This optional method allow users to receive an email notification upon successful completion of the ETL task. It take an object as an argument and show be formatted like this:

```js	
const emailMessage = {
	to: 'to-email-address',
	from: 'from-email-address',
	subject: 'Your subject here',
	text: 'Your body message here',
	html: '<strong>Your html here</strong>',
};
```

Example
```js
.addEmailNotification(emailMessage)
```

#### addTextNotification(textMessage)
This optional method allow users to receive an text notification upon successful completion of the ETL task. It take an object as an argument and show be formatted like this:

```js	
const textMessage = {
		to: 'to-phone-number',
		body: 'YYour body message here',
	}
```

Example
```js
.addTextNotification(emailMessage)
```

NOTE: You'll also need to create a .env on the root of the project. It should include the following:

```
# Twilio account info
TWILIO_ACCOUNT_SID=
TWILIO_AUTH_TOKEN=
TWILIO_PHONE_NUMBER=

# SendGrid account info
SENDGRID_API_KEY= 
```

---

### Scheduling
#### .addSchedule(cronString)
This optional method allows users to schedule a task to be executed repeated using cron. It takes a cron string as a parameter.

Example
```js
.addSchedule('0 * * * * *')
```

---

### Chaining
#### .next(etlInstance)
This optional method allows users to trigger the execution of another ETL task upon successful completion of the current task. It takes an Etl object as an argument.

Example
```js
.next(task2)
```

### Misc
#### .combine()
This method combines the extractor, transformers, and loader, piping each segment to one another and wrapping it in an observable. 

#### .start()
This method Subscribes to the task observable stored in Etl's state triggering the process to begin


