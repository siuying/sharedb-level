const DB = require('sharedb').DB;
const Level = require('level')
const LevelLogs = require('level-logs')
const debug = require('debug')('sharedb-leveldb')

// leveldb-backed ShareDB database

function LevelDB(options) {
  if (!(this instanceof LevelDB)) return new LevelDB(options)
  DB.call(this, options)
  this.db = Level(options)
  this.logs = LevelLogs(this.db, {valueEncoding: 'json'})
}
module.exports = LevelDB

LevelDB.prototype = Object.create(LevelDB.prototype)

LevelDB.prototype.close = function(callback) {
  this.db.close()
  if (callback) callback()
}

// Persists an op and snapshot if it is for the next version. Calls back with
// callback(err, succeeded)
LevelDB.prototype.commit = function(collection, id, op, snapshot, options, callback) {
  const opKey = `op!${collection}!${id}`
  const snapshotKey = `ss!${collection}!${id}`
  this.logs.head(opKey, (err, max_version) => {
    if (err) {
      return callback(err)
    }

    if (snapshot.v !== max_version + 1) {
      return callback(null, false)
    }

    // insert ops
    this.logs.append(opKey, op, (err, version) => {
      if (err) {
        return callback(err)
      }

      this.logs.append(snapshotKey, {id, type: snapshot.type, data: snapshot.data, m: snapshot.m, v: version}, (err, result) => {
        if (err) {
          return callback(err)
        }
        callback(null, true)
      })
    })
  })
}

// Get the named document from the database. The callback is called with (err,
// snapshot). A snapshot with a version of zero is returned if the docuemnt
// has never been created in the database.
LevelDB.prototype.getSnapshot = function(collection, id, fields, options, callback) {
  const includeMetadata = (fields && fields.$submit) || (options && options.metadata)
  const snapshotKey = `ss!${collection}!${id}`
  this.logs.head(snapshotKey, (err, seq) => {
    if (err) {
      return callback(err)
    }
    this.logs.get(snapshotKey, seq, (err, value) => {
      if (err && err.type != 'NotFoundError') {
        return callback(err)
      }
      if (value) {
        if (!includeMetadata) {
          delete value.m
        }
        callback(null, value)
      } else {
        callback(null, {id, v: 0, type: null})
      }
    })
  })
}

// Get operations between [from, to) noninclusively. (Ie, the range should
// contain start but not end).
//
// If end is null, this function should return all operations from start onwards.
//
// The operations that getOps returns don't need to have a version: field.
// The version will be inferred from the parameters if it is missing.
//
// Callback should be called as callback(error, [list of ops]);
LevelDB.prototype.getOps = function(collection, id, from, to, options, callback) {
  const includeMetadata = (options && options.metadata)
  const opKey = `op!${collection}!${id}`
  this.logs.head(opKey, (err, seq) => {
    if (err) {
      debug("err", err)
      return callback(err)
    }

    if (!to) {
      to = seq
    }

    const lastKey = Math.min(to, seq)
    let promises = []
    for (let i = from; i < lastKey; i++) {
      const promise = new Promise((resolve, reject) => {
        this.logs.get(opKey, i, (err, value) => {
          if (err && err.type != 'NotFoundError') {
            return reject(err)
          }
          if (value) {
            if (!includeMetadata) {
              delete value.m
            }
            resolve(value)
          } else {
            resolve({id, v: 0, type: null})
          }
        })
      })
      promises.push(promise)
    }

    Promise.all(promises).then((values) => {
      callback(null, values)
    }).catch((error) => {
      callback(error)
    })
  })
}