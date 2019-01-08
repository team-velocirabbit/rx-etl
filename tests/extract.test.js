const { expect } = require('chai');
const Etl = require('../Etl');


describe('File: Etl.js', () => {
  describe('Method: reset', () => {
    it('Transformers length should equal zero', () => {
      testEtl.reset();
      expect(testEtl.transformers.length).to.equal(0);
    });
  });
});
