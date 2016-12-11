let request = Promise.promisify(require('request'), { multiArgs: true });

let baseUrl = 'https://' + config['export'].account + '.carto.com';

function runAsQuery(q) {
  let options = {
    method : 'GET',
    url    : baseUrl + '/api/v2/sql',

    qs: {
      'api_key' : config['export'].key,
      'q'       : q
    },

    json : true
  };

  return request(options).spread(function (response, body) {
      if (response.statusCode != 200) {
        let error = body ? (body.error ? body.error : body) : '[EMPTY RESPONSE]';
        error = _.isArray(error) ? error : [ error ];

        return Promise.reject(new Error('Remote query failed: ' + error.join('\n')));
      }

      return body.rows;
    });
}

function runAsJob() {
  let options = {
    method : 'POST',
    url    : baseUrl + '/api/v2/sql/job',

    qs: {
      'api_key' : config['export'].key
    },

    headers: {
      'Content-Type' : 'application/json'
    },

    body: {
      query: Array.prototype.slice.call(arguments)
    },

    json : true
  };

  function checkJob(id) {
    if (config['debug']) {
      console.log(new Date().toISOString(), 'CHECKING JOB', id);
    }

    return Promise.try(function () {
        let options = {
          method : 'GET',
          url    : baseUrl + '/api/v2/sql/job/' + id,

          qs: {
            'api_key' : config['export'].key
          },

          json : true
        };

        return request(options).spread(function (response, body) {
            if (response.statusCode != 200) {
              let error = body ? (body.error ? body.error : body) : '[EMPTY RESPONSE]';
              error = _.isArray(error) ? error : [ error ];

              return Promise.reject(new Error('Remote job check failed: ' + error.join('\n')));
            }

            if (body.status == 'done') {
              if (config['debug']) {
                console.log(new Date().toISOString(), 'JOB', id, 'DONE');
              }

              return Promise.resolve();
            }

            if (body.status == 'running') {
              return Promise.delay(config['export'].checkDelay).return(id).then(checkJob);
            }

            if (body.status == 'pending') {
              return Promise.delay(config['export'].checkDelay).return(id).then(checkJob);
            }
          
            return Promise.reject(new Error('Job "' + id + '" unknown status: ' + JSON.stringify(body)));
          });
      });
  };

  return request(options).spread(function (response, body) {
      if (response.statusCode != 201) {
        let error = body ? (body.error ? body.error : body) : '[EMPTY RESPONSE]';
        error = _.isArray(error) ? error : [ error ];

        return Promise.reject(new Error('Remote job failed: ' + error.join('\n')));
      }

      if (config['debug']) {
        console.log(new Date().toISOString(), 'JOB', body.job_id, _.reduce(options.body.query, (a, q) => (a + q.length), 0));
      }

      return checkJob(body.job_id);
    })
}

function makeChunkQuery(chunk) {
  return [
      'INSERT INTO "' + config['export'].table + '"("circumscriptie","sectie","voturi","ocupare") VALUES ',
      chunk.map((v) => '(\'' + [ v.circumscriptie, v.sectie, v.voturi, v.ocupare ].join('\',\'') + '\')').join(','),
      ';'
    ].join('');
}

module.exports = {
  upload: function (records) {
    let parts = [];
    
    for (let i = 0; i < records.length; i += config['export'].chunkSize) {

      parts.push(makeChunkQuery(records.slice(i, i + config['export'].chunkSize)));
    }

    return runAsQuery('TRUNCATE TABLE "' + config['export'].table + '";').then(function () {
        return Promise.map(parts, (p) => runAsJob(p), { concurrency: config['export'].concurrency });
      });
  },

  refresh: function (date) {
    date = date.toISOString();

    function primary() {
      let queries = [
        'UPDATE sectii_tara s SET last_update = \'' + date + '\', nr_votanti = _.voturi, pr_votanti = ROUND(COALESCE(100::real * _.voturi / s.votanti_estimati::integer, 0)::numeric, 2) FROM sync _ WHERE _.circumscriptie = s.nce_judet::smallint AND _.sectie = s.numar_sectie::smallint AND s.nr_votanti <> _.voturi',
        'UPDATE sectii_diaspora s SET last_update = \'' + date + '\', nr_votanti = _.voturi, pr_votanti = ROUND(COALESCE(100::real * _.voturi / NULLIF(s.votanti_estimati::integer, 0), 100::real * _.voturi / NULLIF(_.voturi, 0),  0)::numeric, 2), gr_ocupare = _.ocupare FROM sync _ WHERE _.circumscriptie = s.nr_circumscriptie::smallint AND _.sectie = s.numar_sectie::smallint AND s.nr_votanti <> _.voturi'
      ];

      return Promise.map(queries, runAsQuery, { concurrency: config['export'].concurrency });
    }
    
    function secondary() {
      let queries = [
        'UPDATE tari t SET last_update = _.last, nr_votanti = _.voturi, pr_votanti = ROUND(COALESCE(100::real * _.voturi / NULLIF(t.nr_estimat::integer, 0), 100::real * _.voturi / NULLIF(_.voturi, 0), 0)::numeric, 2) FROM (SELECT s.cod_tara, MAX(s.last_update), SUM(s.nr_votanti) FROM sectii_diaspora s GROUP BY 1) AS _(iso, last, voturi) WHERE _.iso = t.iso_code AND t.nr_votanti <> _.voturi',
        'UPDATE uat u SET last_update = _.last, nr_votanti = _.voturi, pr_votanti = ROUND(COALESCE(100::real * _.voturi / NULLIF(u.votanti_estimati::integer, 0), 100::real * _.voturi / NULLIF(_.voturi, 0), 0)::numeric, 2) FROM (SELECT s.siruta_uat, MAX(s.last_update), SUM(s.nr_votanti) FROM sectii_tara s GROUP BY 1) AS _(siruta, last, voturi) WHERE _.siruta = u.siruta AND u.nr_votanti <> _.voturi',
        'UPDATE judet j SET last_update = _.last, nr_votanti = _.voturi, pr_votanti = ROUND(COALESCE(100::real * _.voturi / NULLIF(j.votanti_estimati::integer, 0), 100::real * _.voturi / NULLIF(_.voturi, 0), 0)::numeric, 2) FROM (SELECT s.siruta_judet::integer, MAX(s.last_update), SUM(s.nr_votanti) FROM sectii_tara s GROUP BY 1) AS _(siruta, last, voturi) WHERE _.siruta = j.siruta_judet AND j.nr_votanti <> _.voturi'
      ];

      return Promise.map(queries, runAsQuery, { concurrency: config['export'].concurrency });
    }

    return primary().then(secondary);
  }
};
