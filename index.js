'use strict';

const express = require('express');
const cassandra = require('cassandra-driver');
const stringify = require('json-stringify');

const client = new cassandra.Client({ contactPoints: ['cassandra'], keyspace: 'kafka_topics' });

const app = express();
app.get('/', (req, res) => {
  const color = req.query.color;

  client.connect()
  .then(function () {
    return color? client.execute('SELECT * FROM small_topic WHERE key = ?', [ color ]) :
                  client.execute('SELECT * FROM small_topic') ;
  })
  .then(function (result) {
    res.send(
      stringify(result.rows, null, 2)
    );
  })
  .catch(function (err) {
    console.error(err);
    res.status(500).end();
    return client.shutdown();
  });

});

app.listen(9300, '0.0.0.0');
console.log(`Running on port 9300`);

process.on('SIGTERM', function () {
  client.shutdown();
});