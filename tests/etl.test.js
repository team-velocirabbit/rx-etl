const { expect } = require('chai');
const Etl = require('../Etl');
const { Observable } = require('rxjs');
const extract = require('../extractors/extract');
const load = require('../loaders/load');

describe('File: Etl.js', () => {
  let testEtl;

  beforeEach(() => {
    testEtl = new Etl();
  });

  describe('Method: reset', () => {
    it('Transformers length should equal zero', () => {
      const testFunc = function () { console.log('test'); };
      testEtl.transformers = [testFunc];
      expect(testEtl.transformers.length).to.not.equal(0);
      testEtl.reset();
      expect(testEtl.transformers.length).to.equal(0);
    });
  });
  
<<<<<<< HEAD


=======
  describe('Method: addSchedule', () => {
    const cronTime = '100 * *';
    it('Should throw an error if an invalid cron format is passed', () => {
      expect(() => testEtl.addSchedule(cronTime)).to.throw('cron format is invalid!');
    });
  });

  describe('Method: start', () => {
    it('Should throw an error if a non-boolean argument is passed', () => {
      const startNow = 'maybe';
      expect(() => testEtl.start(startNow)).to.throw("invalid arg to .start() method! Please make sure arg is type 'boolean'!");
    });
  });

  describe('Method: addTransformers', () => {
    let transformers = ['whatever Jae wants', () => { console.log('whatever Jae wants'); }];
    it('Should throw an error if arguments in transformers array are not functions', () => {
      expect(() => testEtl.addTransformers(transformers)).to.throw("argument must be an array of functions!\n See docs for more details.\n");
    });
    it('Should add same number of functions to state that it was passed in', () => {
      transformers = [() => console.log('testing'), () => 1];
      testEtl.addTransformers(transformers);
      expect(testEtl.transformers.length).to.equal(transformers.length);
    });
  });

  describe('Method: addLoaders', () => {
    it("Should throw an error if the loaders and output file type do not match", () => {
      expect(() => testEtl.addLoaders(load.toCSV, 'MOCK_DATA.json'))
        .to.throw("loading to csv requires output file to be of type '.csv'!");
      expect(() => testEtl.addLoaders(load.toJSON, 'MOCK_DATA.csv'))
        .to.throw("loading to json requires output file to be of type '.json'!");
      expect(() => testEtl.addLoaders(load.toXML, 'MOCK_DATA.csv'))
        .to.throw("loading to xml requires output file to be of type '.xml'!");
      expect(() => testEtl.addLoaders(load.toMongoDB, 'MOCK_DATA.csv'))
        .to.throw("database loaders must provide connection string AND collection / table name in the parameter!");
      expect(() => testEtl.addLoaders(load.toPostgres, 'MOCK_DATA.csv'))
        .to.throw("database loaders must provide connection string AND collection / table name in the parameter!");
    });
  });

  describe('Method: addExtractors', () => {
    it('Should throw error if extractors and output file type do not match', () => {
      expect(() => testEtl.addExtractors(extract.fromCSV, 'MOCK_DATA.json'))
        .to.throw('please make sure extract function matches file type!');
      expect(() => testEtl.addExtractors(extract.fromJSON, 'MOCK_DATA.csv'))
        .to.throw('please make sure extract function matches file type!');
      expect(() => testEtl.addExtractors(extract.fromXML, 'MOCK_DATA.json'))
        .to.throw('please make sure extract function matches file type!');
      expect(() => testEtl.addExtractors(extract.fromMongoDB, 'MOCK_DATA.json'))
        .to.throw('please make sure extract function matches file type!');
      expect(() => testEtl.addExtractors(extract.fromPostgres, 'MOCK_DATA.json'))
        .to.throw('please make sure extract function matches file type!');
    })
    it('Should add extractor$ to state and it should be an Observable', () => {
      const result = testEtl.addExtractors(extract.fromCSV, 'MOCK_DATA.csv');
      expect(result.extractor$).to.not.equal(null);
      expect(result.extractor$).to.be.an.instanceof(Observable);
    });
  });

  describe('Method: simple', () => {
    it('Should throw error if parameters of method are not provided or invalid', () => {
      expect(() => testEtl.simple())
        .to.throw('first parameter of simple() must be a string and cannot be empty!');
      expect(() => testEtl.simple('test', [], 'connectStr', 1))
        .to.throw('fourth parameter of simple() must be a string!');
      expect(() => testEtl.simple('test', [], 1))
        .to.throw('third parameter of simple() must be a string and cannot be empty!');
      expect(() => testEtl.simple('test', 1, 'connectStr'))
        .to.throw('second parameter of simple() must be a function and cannot be empty!');
      expect(() => testEtl.simple(1, () => 1, 'connectStr'))
        .to.throw('first parameter of simple() must be a string and cannot be empty!');
    });
  });
>>>>>>> f50ceffd2500414c1327e08aeb2ae5bb0644270f
});
