var hypercore = require('../..')
var level = require('level-mem')

module.exports = function create (key, opts) {
  function createStorage (name) {
    return level()
  }
  return hypercore(createStorage, key, opts)
}
