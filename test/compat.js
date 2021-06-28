var tape = require('tape')
var hypercore = require('../')
var replicate = require('./helpers/replicate')
var level = require('level-mem')
var concat = require('concat-stream')
var sub = require('subleveldown')
var Storage = require('../lib/storage')

tape('deterministic data and tree', function (t) {
  t.plan(20)

  var expectedTree = Buffer.from(
    '0502570200002807424c414b4532620000000000000000000000000000000000ab27d45f509274' +
    'ce0d08f4f09ba2d0e0d8df61a0c2a78932e81b5ef26ef398df0000000000000001064321a8413b' +
    'e8c604599689e2c7a59367b031b598bceeeb16556a8f3252e0de000000000000000294c1705400' +
    '5942a002c7c39fbb9c6183518691fb401436f1a2f329b380230af800000000000000018dfe81d5' +
    '76464773f848b9aba1c886fde57a49c283ab57f4a297d976d986651e00000000000000041d2fad' +
    'c9ce604c7e592949edc964e45aaa10990d7ee53328439ef9b2cf8aa6ff00000000000000013a8d' +
    'cc74e80b8314e8e13e1e462358cf58cf5fc4413a9b18a891ffacc551c395000000000000000228' +
    '28647a654a712738e35f49d1c05c676010be0b33882affc1d1e7e9fee59d400000000000000001' +
    '000000000000000000000000000000000000000000000000000000000000000000000000000000' +
    '00baac70b6d38243efa028ee977c462e4bec73d21d09ceb8cc16f4d4b1ee228a45000000000000' +
    '0001d1b021632c7fab84544053379112ca7b165bb21283821816c5b6c89ff7f78e2d0000000000' +
    '000002d2ab421cece792033058787a5ba72f3a701fddc25540d5924e9819d7c12e02f200000000' +
    '00000001',
    'hex'
  )

  for (var i = 0; i < 5; i++) run()

  function run () {
    var st = storage()
    var feed = hypercore(st)

    feed.append(['a', 'b', 'c', 'd', 'e', 'f'], function () {
      readAll(st, 'data', (err, buf) => {
        t.error(err)
        t.same(buf.toString(), 'abcdef', 'data matches')
      })
      readAll(st, 'tree', (err, buf) => {
        t.error(err)
        t.same(buf, expectedTree, 'tree matches')
      })
    })
  }
})

tape('deterministic data and tree after replication', function (t) {
  t.plan(20)

  var expectedTree = Buffer.from(
    '0502570200002807424c414b4532620000000000000000000000000000000000ab27d45f509274' +
    'ce0d08f4f09ba2d0e0d8df61a0c2a78932e81b5ef26ef398df0000000000000001064321a8413b' +
    'e8c604599689e2c7a59367b031b598bceeeb16556a8f3252e0de000000000000000294c1705400' +
    '5942a002c7c39fbb9c6183518691fb401436f1a2f329b380230af800000000000000018dfe81d5' +
    '76464773f848b9aba1c886fde57a49c283ab57f4a297d976d986651e00000000000000041d2fad' +
    'c9ce604c7e592949edc964e45aaa10990d7ee53328439ef9b2cf8aa6ff00000000000000013a8d' +
    'cc74e80b8314e8e13e1e462358cf58cf5fc4413a9b18a891ffacc551c395000000000000000228' +
    '28647a654a712738e35f49d1c05c676010be0b33882affc1d1e7e9fee59d400000000000000001' +
    '000000000000000000000000000000000000000000000000000000000000000000000000000000' +
    '00baac70b6d38243efa028ee977c462e4bec73d21d09ceb8cc16f4d4b1ee228a45000000000000' +
    '0001d1b021632c7fab84544053379112ca7b165bb21283821816c5b6c89ff7f78e2d0000000000' +
    '000002d2ab421cece792033058787a5ba72f3a701fddc25540d5924e9819d7c12e02f200000000' +
    '00000001',
    'hex'
  )

  for (var i = 0; i < 5; i++) run()

  function run () {
    var feed = hypercore(storage())

    feed.append(['a', 'b', 'c', 'd', 'e', 'f'], function () {
      var st = storage()
      st.log = true
      var clone = hypercore(st, feed.key)

      replicate(feed, clone).on('end', function () {
        readAll(st, 'data', (err, buf) => {
          t.error(err)
          t.same(buf.toString(), 'abcdef')
        })
        readAll(st, 'tree', (err, buf) => {
          t.error(err)
          t.same(buf.toString('hex'), expectedTree.toString('hex'))
        })
      })
    })
  }
})

tape('deterministic signatures', function (t) {
  t.plan(30)

  var key = Buffer.from('9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238', 'hex')
  var secretKey = Buffer.from(
    '53729c0311846cca9cc0eded07aaf9e6689705b6a0b1bb8c3a2a839b72fda383' +
    '9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238',
    'hex'
  )

  var compatExpectedSignatures = Buffer.from(
    '050257010000400745643235353139000000000000000000000000000000000084684e8dd76c339' +
    'd6f5754e813204906ee818e6c6cdc6a816a2ac785a3e0d926ac08641a904013194fe6121847b7da' +
    'd4e361965d47715428eb0a0ededbdd5909d037ff3c3614fa0100ed9264a712d3b77cbe7a4f6eadd' +
    '8f342809be99dfb9154a19e278d7a5de7d2b4d890f7701a38b006469f6bab1aff66ac6125d48baf' +
    'dc0711057675ed57d445ce7ed4613881be37ebc56bb40556b822e431bb4dc3517421f9a5e3ed124' +
    'eb5c4db8367386d9ce12b2408613b9fec2837022772a635ffd807',
    'hex'
  )

  var expectedSignature = Buffer.from(
    '42e057f2c225b4c5b97876a15959324931ad84646a8bf2e4d14487c0f117966a585edcdda54670d' +
    'd5def829ca85924ce44ae307835e57d5729aef8cd91678b06',
    'hex'
  )

  for (var i = 0; i < 5; i++) run()

  function run () {
    var st = storage()
    var feed = hypercore(st, key, {
      secretKey: secretKey
    })

    feed.append(['a', 'b', 'c'], function () {
      readAll(st, 'data', (err, buf) => {
        t.error(err)
        t.same(buf.toString(), 'abc')
      })
      feed.verify(feed.length - 1, compatExpectedSignatures.slice(-64), function (err, valid) {
        t.error(err, 'no error')
        t.ok(valid, 'compat sigs still valid')
      })
      readAll(st, 'signatures', (err, buf) => {
        t.error(err)
        t.same(buf.slice(-64), expectedSignature, 'only needs last sig')
      })
    })
  }
})

tape('deterministic signatures after replication', function (t) {
  t.plan(20)

  var key = Buffer.from('9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238', 'hex')
  var secretKey = Buffer.from(
    '53729c0311846cca9cc0eded07aaf9e6689705b6a0b1bb8c3a2a839b72fda383' +
    '9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238',
    'hex'
  )

  var expectedSignature = Buffer.from(
    '42e057f2c225b4c5b97876a15959324931ad84646a8bf2e4d14487c0f117966a585edcdda54670d' +
    'd5def829ca85924ce44ae307835e57d5729aef8cd91678b06',
    'hex'
  )

  for (var i = 0; i < 5; i++) run()

  function run () {
    var feed = hypercore(storage(), key, {
      secretKey: secretKey
    })

    feed.append(['a', 'b', 'c'], function () {
      var st = storage()
      var clone = hypercore(st, feed.key)

      replicate(feed, clone).on('end', function () {
        readAll(st, 'data', (err, buf) => {
          t.error(err)
          t.same(buf.toString(), 'abc')
        })
        readAll(st, 'signatures', (err, buf) => {
          t.error(err)
          t.same(buf.slice(-64), expectedSignature, 'only needs last sig')
        })
      })
    })
  }
})

tape('compat signatures work', function (t) {
  var key = Buffer.from('9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238', 'hex')
  var secretKey = Buffer.from(
    '53729c0311846cca9cc0eded07aaf9e6689705b6a0b1bb8c3a2a839b72fda383' +
    '9718a1ff1c4ca79feac551c0c7212a65e4091278ec886b88be01ee4039682238',
    'hex'
  )

  var compatExpectedSignatures = Buffer.from(
    '050257010000400745643235353139000000000000000000000000000000000084684e8dd76c339' +
    'd6f5754e813204906ee818e6c6cdc6a816a2ac785a3e0d926ac08641a904013194fe6121847b7da' +
    'd4e361965d47715428eb0a0ededbdd5909d037ff3c3614fa0100ed9264a712d3b77cbe7a4f6eadd' +
    '8f342809be99dfb9154a19e278d7a5de7d2b4d890f7701a38b006469f6bab1aff66ac6125d48baf' +
    'dc0711057675ed57d445ce7ed4613881be37ebc56bb40556b822e431bb4dc3517421f9a5e3ed124' +
    'eb5c4db8367386d9ce12b2408613b9fec2837022772a635ffd807',
    'hex'
  )

  var st = storage()

  var feed = hypercore(st, key, {
    secretKey
  })

  function writeSignatures (cb) {
    const batch = st.signatures.batch()
    batch.put(0, compatExpectedSignatures.slice(0, 32))
    var sigCount = (compatExpectedSignatures.length - 32 / 64)
    for (var i = 0; i < sigCount; i++) {
      batch.put(i + 1, compatExpectedSignatures.slice(32 + i * 64, 32 + i * 64 + 64))
    }
    batch.write(cb)
  }

  feed.append(['a', 'b', 'c'], function () {
    writeSignatures(function () {
      var clone = hypercore(storage(), key)

      replicate(feed, clone).on('end', function () {
        t.same(clone.length, 3)
        clone.proof(2, function (err, proof) {
          t.error(err)
          t.same(proof.signature, compatExpectedSignatures.slice(-64))

          feed.append('d', function () {
            replicate(feed, clone).on('end', function () {
              t.same(clone.length, 4)
              t.end()
            })
          })
        })
      })
    })
  })
})

function storage () {
  return create

  function create (name) {
    const db = level()
    create[name] = sub(db, '', Storage.encoding)
    return db
  }
}

function readAllData (db, cb) {
  var valueStream = db.createValueStream()
  valueStream.pipe(concat(buf => cb(null, buf)))
}

var valueSizes = {
  tree: 40,
  signatures: 64
}

/**
 * A bit of a hack to make these tests work without too much modification. In
 * the original hypercore storage, these are sparse files, so empty values are
 * zeros. In level storage, an empty value does not exist. This function writes
 * a buffer that matches what would be expected in the original hypercore
 * fixtures
 */
function readAll (st, name, cb) {
  if (name === 'data') return readAllData(st[name], cb)
  var db = st[name]
  var rs = db.createReadStream()
  var valueSize = valueSizes[name]
  if (typeof valueSize !== 'number') throw new Error('unknown db name ' + name)
  rs.pipe(concat((data) => {
    var length = data[data.length - 1].key
    var buf = Buffer.alloc(32 + valueSize * length)
    for (const { key, value } of data) {
      var offset = key === 0 ? 0 : 32 + (key - 1) * valueSize
      value.copy(buf, offset)
    }
    cb(null, buf)
  }))
}
