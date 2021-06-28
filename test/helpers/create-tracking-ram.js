var level = require('level-mem')

module.exports = function () {
  var logByFilename = {}
  var factory = function (filename) {
    var memory = level()
    var log = []
    logByFilename[filename] = log
    var tracked = {}
    for (var key of Object.keys(memory)) {
      tracked[key] = logAndForward(key)
    }
    return tracked

    function logAndForward (op) {
      return function () {
        var statement = {}
        statement[op] = [].slice.apply(arguments)
        statement[op].pop()
        log.push(statement)
        return memory[op].apply(memory, arguments)
      }
    }
  }
  factory.log = logByFilename
  return factory
}
