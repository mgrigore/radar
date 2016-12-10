let fs   = Promise.promisifyAll(require('fs'))
  , path = require('path')
  , copy = require('pg-copy-streams');


function getTimestamp(fileName) {
  let match = /^presence_(\d{4})-(\d{2})-(\d{2})_(\d{2})-(\d{2})\.csv$/.exec(fileName);

  if (match) {
    return Promise.resolve([ fileName, new Date(match[1], parseInt(match[2]) - 1, match[3], match[4], match[5]) ]);
  }

  return fs.statAsync(path.resolve(config.dataDir, fileName)).then((stats) => [ fileName, stats.mtime ]);
}

function ensureStorage(fileName) {
  let table = '"' + config['import'].schema + '"."' + fileName + '"'
    , q     = 'DROP TABLE IF EXISTS ' + table  + '; CREATE TABLE ' + table + '(';

  let columns = [];

  for (entry in config['import'].columns) {
    for (column in config['import'].columns[entry]) {
      columns.push('"' + column + '" ' + config['import'].columns[entry][column]);
    }
  }

  q += columns.join(', ');

  q += ');';

  return db.query(q).return(table);
}

module.exports = {
  list: function () {
    return fs.readdirAsync(config.dataDir).map(getTimestamp).then(_.object);
  },

  update: function (file) {
    return ensureStorage(file.name).then(function (target) {
        file.data = target;

        let command = 'COPY ' + target + ' FROM STDIN'
          , options = [];

        if (config['import'].command['FORMAT']) {
          options.push('FORMAT \'' + config['import'].command['FORMAT'] + '\'');
        }

        if (undefined !== config['import'].command['OIDS']) {
          options.push('OIDS \'' + (config['import'].command['FORMAT'] ? 'TRUE' : 'FALSE') + '\'');
        }

        if (config['import'].command['DELIMITER']) {
          options.push('DELIMITER \'' + config['import'].command['DELIMITER'] + '\'');
        }

        if (config['import'].command['NULL']) {
          options.push('NULL \'' + config['import'].command['NULL'] + '\'');
        }

        if (undefined !== config['import'].command['HEADER']) {
          options.push('HEADER \'' + (config['import'].command['HEADER'] ? 'TRUE' : 'FALSE') + '\'');
        }

        if (config['import'].command['QUOTE']) {
          options.push('QUOTE \'' + config['import'].command['QUOTE'] + '\'');
        }

        if (config['import'].command['ESCAPE']) {
          options.push('ESCAPE \'' + config['import'].command['ESCAPE'] + '\'');
        }

        if (config['import'].command['FORCE_QUOTE']) {
          options.push('FORCE_QUOTE ' + config['import'].command['FORCE_QUOTE']);
        }

        if (config['import'].command['FORCE_NOT_NULL']) {
          options.push('FORCE_NOT_NULL ' + config['import'].command['FORCE_NOT_NULL'] );
        }

        if (config['import'].command['ENCODING']) {
          options.push('ENCODING \'' + config['import'].command['ENCODING'] + '\'');
        }

        if (options.length) {
          command += ' (' + options.join(', ') + ')';
        }

        return new Promise(function (resolve, reject) {
            db.connect(function (err, client, done) {
              if (err) {
                return reject(err);
              }

              let streamOut = client.query(copy.from(command))
                , streamIn  = fs.createReadStream(path.resolve(config.dataDir, file.name));
                
              streamIn.on ('error', (err) => { done(); reject(err); });
              streamOut.on('error', (err) => { done(); reject(err); });

              streamOut.on('end', () => { done(); resolve(); });

              streamIn.pipe(streamOut);
            });
          }).then(function () {
            return db.run('deleteSource', { name: file.name });
          }).then(function () {
            return db.run('upsertSource', file);
          });
      });
  },

  latest: function () {
    return db.run('latestSource').get(0).get('latest');
  }
};
