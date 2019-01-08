const { expect } = require('chai');
const Etl = require('../Etl');


describe('File: Etl.js', () => {
  let testEtl;

  beforeEach(() => {
    const testFunc = function () { console.log('test'); };
    testEtl = new Etl();
    testEtl.transformers = [testFunc];
  });

  describe('Method: reset', () => {
    it('Transformers length should equal zero', () => {
      testEtl.reset();
      expect(testEtl.transformers.length).to.equal(0);
    });
  });
  


});
