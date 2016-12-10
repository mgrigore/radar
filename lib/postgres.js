let pg   = require('pg')
  , path = require('path');

let readFile = Promise.promisify(require('fs').readFile);

function fetchScript(script) {
  if (!script) {
    return Promise.reject(new Error('Invalid script'));
  }

  let fullPath = path.resolve(fetchScript.root, /\.sql$/.test(script) ? script : (script + '.sql'));
  
  if (fetchScript.cache[fullPath]) {
    return Promise.resolve(fetchScript.cache[fullPath]);
  }

  return readFile(fullPath, { encoding: 'utf-8' }).then(function (result) {
      if (result.charCodeAt(0) === 0xFEFF) {
		    return result.slice(1);
	    }

      return result;
    }).tap(function (result) {
      fetchScript.cache[fullPath] = result;
    });
}

fetchScript.root  = './sql/';
fetchScript.cache = {};

let _submit = pg.Query.prototype.submit;

pg.Query.prototype.submit = function (connection) {
  if (config['debug']) {
    console.log(new Date().toISOString(), 'SQL', this.text);
  }

  _submit.call(this, connection);
};

pg.Pool.prototype.run = function (script, args) {
  return fetchScript(script).bind(this).then(function (sql) {
      let map    = Object.create(null)
        , values = [];

      for (arg in args) {
        values.push(args[arg]); 
        map[arg] = values.length;
      }

      let parsed = sql.replace(/(^|[^\w$])\$(\w+)([^\w$]|$)/g, (m, pre, arg, post) => (pre + '$' + (map[arg] || arg) + post))
                      .replace(/\r?\n/g, ' ')
                      .replace(/;/g, ';\n');

      return this.query(parsed, values).get('rows');
    });
}

module.exports = new pg.Pool(config.pg);
