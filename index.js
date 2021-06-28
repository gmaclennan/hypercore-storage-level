const Hypercore = require('hypercore')
const Storage = require('./lib/storage')

module.exports = function (createStorage, key, opts) {
  if (typeof key === 'string') key = Buffer.from(key, 'hex')

  if (!Buffer.isBuffer(key) && !opts) {
    opts = key
    key = null
  }

  if (!opts) opts = {}

  opts.storage = new Storage(createStorage, opts)
  return new Hypercore(null, key, opts)
}
