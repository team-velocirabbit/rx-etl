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

```
const Etl = require('rxjs-etl');
```

Sample configuration .csv -> mongodb
```
	const task1 = new Etl()
		.addExtractors(extract.fromCSV, 'SAMPLE_DATA.csv')
		.addTransformers(transformFunction)
		.addLoaders(load.toMongoDB, mongoURI, collectionName)
		.combine()
		.addEmailNotification(email)
		.addTextNotification(text)
		.addSchedule('0 * * * * *')
		.next(task2)
		.start()
 ```
## Extract
