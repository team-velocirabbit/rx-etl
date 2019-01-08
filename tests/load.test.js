const { expect } = require('chai');
const load = require('../loaders/load');

describe('File: load.js', () => {
  describe('Method: toCSV', () => {
    it('should throw error when length of data is 0', () => {
      expect(() => load.toCSV([], 'test', 'test', 0)).to.throw('No data was passed into the load method!');
    });
    it('should throw an error when file is not a CSV file', () => {
      expect(() => load.toCSV([1, 2], 'test', 'test.json', 0)).to.throw('File does not appear to be CSV.');
    });
  });

  describe('Method: toJSON', () => {
    it('should throw error when length of data is 0', () => {
      expect(() => load.toJSON([], 'test', 'test', 0)).to.throw('No data was passed into the load method!');
    });
    it('should throw an error when file is not a JSON file', () => {
      expect(() => load.toJSON([1, 2], 'test', 'test.csv', 0)).to.throw('File does not appear to be JSON.');
    });
  });

  describe('Method: toXML', () => {
    it('should throw error when length of data is 0', () => {
      expect(() => load.toXML([], 'test', 'test', 0)).to.throw('No data was passed into the load method!');
    });
    it('should throw an error when file is not a XML file', () => {
      expect(() => load.toXML([1, 2], 'test', 'test.json', 0)).to.throw('File does not appear to be XML.');
    });
  });

  describe('Method: toMongoDB', () => {
    it('should throw error when invalid connection URI is passed in', () => {
      expect(() => load.toMongoDB([], 'www.yahoo.com/', 'test')).to.throw(Error);
    });
  });

});
