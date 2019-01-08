const { expect } = require('chai');
const { Observable } = require('rxjs');
const extract = require('../extractors/extract');

require('dotenv').config;

describe('File: extract.js', () => {
  describe('Method: fromCSV', () => {
    it('Should throw an error if no file path string is passed in', () => {
      expect(() => extract.fromCSV().to.throw('A file path does not appear to have been passed.'));
    });
    it('Passing a file extension other than CSV should throw an error', () => {
      expect(() => extract.fromCSV('MOCK_DATA.json')).to.throw('File does not appear to be CSV.');
    });
    it('Should return an observable', () => {
      const result = extract.fromCSV('MOCK_DATA.csv');
      expect(result).to.be.an.instanceof(Observable);
    });
  });

  describe('Method: fromJSON', () => {
    it('Should throw an error if no file path string is passed in', () => {
      expect(() => extract.fromJSON().to.throw('A file path does not appear to have been passed.'));
    });
    it('Passing a file extension other than JSON should throw an error', () => {
      expect(() => extract.fromJSON('MOCK_DATA.csv')).to.throw('File does not appear to be JSON.');
    });
    it('Should return an observable', () => {
      const result = extract.fromJSON('MOCK_DATA.json');
      expect(result).to.be.an.instanceof(Observable);
    });
  });

  describe('Method: fromXML', () => {
    it('Should throw an error if no file path string is passed in', () => {
      expect(() => extract.fromXML().to.throw('A file path does not appear to have been passed.'));
    });
    it('Passing a file extension other than XML should throw an error', () => {
      expect(() => extract.fromXML('MOCK_DATA.csv')).to.throw('File does not appear to be XML.');
    });
    it('Should return an observable', () => {
      const result = extract.fromXML('MOCK_DATA.xml');
      expect(result).to.be.an.instanceof(Observable);
    });
  });

  describe('Method: fromMongoDB', () => {
    it('Passing in a non-MongoDB URI should throw an error', () => {
      expect(() => extract.fromMongoDB(process.env.POSTGRES_URI)).to.throw('Connection string does not appear to use the Mongo protocol!');
    });
    it('Should throw an error if no connection string is passed in', () => {
      expect(() => extract.fromMongoDB().to.throw('You must provide a valid connection string!'));
    });
    it('Should throw an error if no collection name is passed in', () => {
      expect(() => extract.fromMongoDB(process.env.MONGO_URI).to.throw('You must provide a valid collection name!'));
    });
    it('Should return an observable', () => {
      const result = extract.fromMongoDB(process.env.MONGO_URI, 'pleasework');
      expect(result).to.be.an.instanceof(Observable);
    });
  });

  describe('Method: fromPostgres', () => {
    it('Passing in a non-Postgres URI should throw an error', () => {
      expect(() => extract.fromPostgres(process.env.MONGO_URI)).to.throw('Connection string does not appear to use the Postgres protocol!');
    });
    it('Should throw an error if no connection string is passed in', () => {
      expect(() => extract.fromPostgres().to.throw('You must provide a valid connection string!'));
    });
    it('Should throw an error if no collection name is passed in', () => {
      expect(() => extract.fromPostgres(process.env.POSTGRES_URI).to.throw('You must provide a valid table name!'));
    });
    it('Should return an observable', () => {
      const result = extract.fromPostgres(process.env.POSTGRES_URI, 'test');
      expect(result).to.be.an.instanceof(Observable);
    });
  });
});
