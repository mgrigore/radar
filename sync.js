process.on('uncaughtException', function (err) {
  console.error(new Date().toISOString(), 'FATAL', err);

  process.exit(-1);
});

let startTime = new Date().getTime() / 1000;

global.resolve = function(name) {
  return require({
    }[name] || ('./' + name));
};

global.Promise = require('bluebird');
global._       = require('underscore');

global.config = require('./config.json');

config['dataDir'] = config['dataDir'] || process.env.DATADIR;

config['pg'].host = config['pg'].host || process.env.PGHOST;
config['pg'].port = config['pg'].port || process.env.PGPORT;

config['pg'].database = config['pg'].database || process.env.PGDATABASE;
config['pg'].user     = config['pg'].user     || process.env.PGUSER;
config['pg'].password = config['pg'].password || process.env.PGPASSWORD;

config['export'].key = config['export'].key || process.env.EXPORTKEY;

global.db = resolve('lib/postgres');

let sources = resolve('lib/sources')
  , carto   = resolve('lib/carto');

console.log(new Date().toISOString(), 'STARTED');

Promise.all([
  sources.list(),
  db.run('getSources')
]).spread(function (files, live) {
  console.log(new Date().toISOString(), 'UPDATING');

  let update = []
    , index  = _.indexBy(live, 'name');

  for (file in files) {
    if (!index[file]) {
      update.push({ name: file, modified: files[file] });
    } else if (index[file].modified < files[file]) {
      update.push({ name: file, modified: files[file] });
    }
  }

  return Promise.all(update.map(sources.update));
}).tap(function () {
  return db.run('refreshMapGenerator');
}).then(function () {
  return sources.latest();
}).then(function (latest) {
  console.log(new Date().toISOString(), 'GENERATING MAP');

  return db.run('generateMap', { latest: latest, minutes: 30, idle: 15, overload: 90 }).then(function (records) {
      console.log(new Date().toISOString(), 'UPLOADING');

      return carto.upload(records);
    }).tap(function () {
      console.log(new Date().toISOString(), 'REFRESHING MAP');

      return carto.refresh(latest);
    });
}).catch(function (err) {
  console.error(new Date().toISOString(), 'ERROR', err);
}).finally(function () {
  db.end();
  
  let stopTime = new Date().getTime() / 1000;

  console.log(new Date().toISOString(), 'DONE', (stopTime - startTime).toFixed(3) + ' seconds');
});
