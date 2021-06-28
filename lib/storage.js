var uint64be = require('uint64be')
var createCache = require('./cache')
var level = require('level')
var sub = require('subleveldown')
var lexint = require('lexicographic-integer-encoding')('hex')

const encodingOptions = { valueEncoding: 'binary', keyEncoding: lexint }
Storage.encoding = encodingOptions

module.exports = Storage

/**
 * @param {string | ((name: string) => import('levelup').LevelUp)} createStorage
 * Pathname for database storage, or a function called with a storage name, that
 * will return a levelUp instance
 * @param {any} [opts] Options for Cache
 */
function Storage (createStorage, opts = {}) {
  if (!(this instanceof Storage)) return new Storage(createStorage, opts)

  if (typeof createStorage === 'string') {
    this._db = level(createStorage)
    createStorage = defaultStorage(this._db)
  }
  if (typeof createStorage !== 'function') throw new Error('Storage should be a function or string')

  const cache = createCache(opts)

  this.treeCache = cache.tree || null
  this.dataCache = cache.data || null
  this._key = null
  this._secretKey = null
  this._tree = null
  this._data = null
  this._bitfield = null
  this._signatures = null
  this._create = function (name) {
    return sub(createStorage(name), '', encodingOptions)
  }
  this._log = createStorage.log
}

Storage.prototype.putData = function (index, data, nodes, cb) {
  if (!cb) cb = noop
  if (!data.length) return cb(null)
  this._data.put(index, data, cb)
}

// Warning: Does not check data size like putData does
Storage.prototype.putDataBatch = function (index, dataBatch, opts, cb) {
  if (typeof cb === 'undefined' && typeof opts === 'function') {
    cb = opts
    opts = {}
  }
  opts = opts || {}
  const ops = dataBatch.map((value, offset) => ({ type: 'put', key: index + offset, value }))
  this._data.batch(ops, cb)
}

Storage.prototype.getData = function (index, cb) {
  var self = this
  var cached = this.dataCache && this.dataCache.get(index)
  if (cached) return process.nextTick(cb, null, cached)
  this._data.get(index, function (err, data) {
    if (err) console.log('data', index, err)
    if (err) return cb(err)
    if (self.dataCache) self.dataCache.set(index, data)
    return cb(null, data)
  })
}

Storage.prototype.clearData = function (start, end, opts, cb) {
  if (typeof end === 'function') return this.clearData(start, start + 1, null, end)
  if (typeof opts === 'function') return this.clearData(start, end, null, opts)
  if (!opts) opts = {}
  if (!end) end = start + 1
  if (!cb) cb = noop

  clearRange(this._data, { gte: start, lte: end }, cb)
}

Storage.prototype.nextSignature = function (index, cb) {
  var self = this

  this._getSignature(index, function (err, signature) {
    if (err) console.log(index, err)
    if (err && !err.notFound) return cb(err)
    if ((err && err.notFound) || isBlank(signature)) return self.nextSignature(index + 1, cb)
    cb(null, { index: index, signature: signature })
  })
}

Storage.prototype.getSignature = function (index, cb) {
  this._getSignature(index, function (err, signature) {
    if (err && err.notFound) return cb(new Error('No signature found'))
    if (err) console.log('sig', index, err)
    if (err) return cb(err)
    if (isBlank(signature)) return cb(new Error('No signature found'))
    cb(null, signature)
  })
}

// Caching not enabled for signatures because they are rarely reused.
Storage.prototype._getSignature = function (index, cb) {
  this._signatures.get(index + 1, cb)
}

Storage.prototype.putSignature = function (index, signature, cb) {
  this._signatures.put(index + 1, signature, cb)
}

Storage.prototype.deleteSignatures = function (start, end, cb) {
  clearRange(this._signatures, { gte: start + 1, lte: end + 1 }, cb)
}

// Caching not enabled for batch reads because they'd be complicated to batch and they're rarely used.
Storage.prototype.getDataBatch = function (start, n, cb) {
  var valueStream = this._data.createValueStream({ gte: start, lt: start + n })
  concatObject(valueStream, cb)
}

Storage.prototype.getNode = function (index, cb) {
  if (this.treeCache) {
    var cached = this.treeCache.get(index)
    if (cached) return cb(null, cached)
  }

  var self = this

  this._tree.get(index + 1, function (err, buf) {
    if (err && err.notFound) return cb(new Error('No node found'))
    if (err) console.log('tree', index, err)
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && isBlank(hash)) return cb(new Error('No node found'))

    var val = new Node(index, hash, size)
    if (self.treeCache) self.treeCache.set(index, val)
    cb(null, val)
  })
}

Storage.prototype.putNodeBatch = function (index, nodes, cb) {
  if (!cb) cb = noop
  if (this._log) console.log(nodes)

  var batch = this._tree.batch()

  for (var i = 0; i < nodes.length; i++) {
    var node = nodes[i]
    var buf = Buffer.alloc(40)
    if (node) {
      node.hash.copy(buf, 0)
      uint64be.encode(node.size, buf, 32)
    }
    if (this._log) console.log('NODE', i + index)
    batch.put(i + index + 1, buf)
  }

  batch.write(cb)
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop
  if (this._log && index === 7) console.log('NODE', index, node)

  // TODO: re-enable put cache. currently this causes a memleak
  // because node.hash is a slice of the big data buffer on replicate
  // if (this.cache) this.cache.set(index, node)

  var buf = Buffer.allocUnsafe(40)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)
  this._tree.put(index + 1, buf, cb)
}

Storage.prototype.putBitfield = function (offset, data, cb) {
  var self = this
  this._bitfield.get(0, function (err, h) {
    if (err) return cb(err)
    var bitfieldPageSize = h.readUInt16BE(5)
    var start = offset / bitfieldPageSize
    if (!Number.isInteger(start)) throw new Error('Something up with pageSize')
    var n = data.length / bitfieldPageSize
    if (!Number.isInteger(n)) throw new Error('Something up with pageSize')

    var batch = self._bitfield.batch()

    for (var i = start; i < start + n; i++) {
      batch.put(i + 1, data.slice(i - start, i - start + bitfieldPageSize))
    }

    batch.write(cb)
  })
}

Storage.prototype.delBitfield = function (cb) {
  clearRange(this._bitfield, { gte: 1 }, cb)
}

Storage.prototype.close = function (cb) {
  if (!cb) cb = noop
  var missing = 6
  var error = null

  close(this._bitfield, done)
  close(this._tree, done)
  close(this._data, done)
  close(this._key, done)
  close(this._secretKey, done)
  close(this._signatures, done)

  if (this._db) {
    missing++
    close(this._db, done)
  }

  function done (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Storage.prototype.destroy = function (cb) {
  if (!cb) cb = noop
  var missing = 6
  var error = null

  destroy(this._bitfield, done)
  destroy(this._tree, done)
  destroy(this._data, done)
  destroy(this._key, done)
  destroy(this._secretKey, done)
  destroy(this._signatures, done)

  function done (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Storage.prototype.openKey = function (opts, cb) {
  if (typeof opts === 'function') return this.openKey({}, opts)
  if (!this._key) this._key = this._create('key')
  this._key.get(0, cb)
}

Storage.prototype.writeKey = function (key, cb) {
  this._key.put(0, key, cb)
}

Storage.prototype.writeSecretKey = function (key, cb) {
  this._secretKey.put(0, key, cb)
}

Storage.prototype.open = function (opts, cb) {
  if (typeof opts === 'function') return this.open({}, opts)

  var self = this
  var error = null
  var missing = 5

  if (!this._key) this._key = this._create('key')
  if (!this._secretKey) this._secretKey = this._create('secret_key')
  if (!this._tree) this._tree = this._create('tree')
  if (!this._data) this._data = this._create('data')
  if (!this._bitfield) this._bitfield = this._create('bitfield')
  if (!this._signatures) this._signatures = this._create('signatures')

  var result = {
    bitfield: [],
    bitfieldPageSize: 3584, // we upgraded the page size to fix a bug
    secretKey: null,
    key: null
  }

  this._bitfield.get(0, function (err, h) {
    // TODO: What locking mechanism do we need?
    // @ts-ignore
    if (err && err.code === 'ELOCKED') return cb(err)
    if (h) result.bitfieldPageSize = h.readUInt16BE(5)
    self._bitfield.put(0, header(0, result.bitfieldPageSize, null), function (err) {
      if (err) return cb(err)
      var valueStream = self._bitfield.createValueStream({ gte: 1 })
      concatObject(valueStream, function (err, pages) {
        if (pages) result.bitfield = pages
        done(err)
      })
    })
  })

  this._signatures.put(0, header(1, 64, 'Ed25519'), done)
  this._tree.put(0, header(2, 40, 'BLAKE2b'), done)

  // TODO: Improve the error handling here.
  // I.e. if secretKey length === 64 and it fails, error

  this._secretKey.get(0, function (_, data) {
    if (data) result.secretKey = data
    done(null)
  })

  this._key.get(0, function (_, data) {
    if (data) result.key = data
    done(null)
  })

  function done (err) {
    if (err) error = err
    if (--missing) return
    if (error) cb(error)
    else cb(null, result)
  }
}

Storage.Node = Node

function noop () {}

function header (type, size, name) {
  var buf = Buffer.alloc(32)

  // magic number
  buf[0] = 5
  buf[1] = 2
  buf[2] = 87
  buf[3] = type

  // version
  buf[4] = 0

  // block size
  buf.writeUInt16BE(size, 5)

  if (name) {
    // algo name
    buf[7] = name.length
    buf.write(name, 8)
  }

  return buf
}

function Node (index, hash, size) {
  this.index = index
  this.hash = hash
  this.size = size
}

function isBlank (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (buf[i]) return false
  }
  return true
}

function close (st, cb) {
  if (st.close) st.close(cb)
  else cb()
}

function destroy (st, cb) {
  if (st.destroy) st.destroy(cb)
  else cb()
}

/**
 * @param {import('levelup').LevelUp} db
 * @param {import('abstract-leveldown').AbstractIteratorOptions} opts
 */
function clearRange (db, opts, cb) {
  var keyStream = db.createKeyStream(opts)
  concatObject(keyStream, function (err, keys) {
    if (err) return cb(err)
    var ops = keys.map(key => ({ type: 'del', key }))
    db.batch(ops, cb)
  })
}

function concatObject (stream, cb) {
  var chunks = []
  stream.on('data', function (chunk) {
    chunks.push(chunk)
  })
  stream.once('end', function () {
    if (cb) cb(null, chunks)
    cb = null
  })
  stream.once('error', function (err) {
    if (cb) cb(err)
    cb = null
  })
}
/**
 * @param {import('levelup').LevelUp} db
 * @returns {(name: string) => import('levelup').LevelUp}
 */
function defaultStorage (db) {
  return function (name) {
    return sub(db, name)
  }
}
