'use strict';

const mysql = require('mysql2');
const uuid = require('uuid');

function DBQueue(attrs) {
  this.table  = attrs.table_name || 'jobs';
  this.worker = uuid.v4();

  this.serializer   = attrs.serializer   || JSON.stringify;
  this.deserializer = attrs.deserializer || JSON.parse;
  this.persist_last_error = attrs.persist_last_error || false;

  delete attrs.table_name;
  const pool = mysql.createPoolPromise(attrs);
  pool.on('connection', function(conn) {
    conn.query('SET sql_mode="STRICT_ALL_TABLES"', [])
  });

  this.db = pool;
}

DBQueue.connect = async function(options) {
  const queue = new DBQueue(options);

  await queue.query("SELECT NOW()", []);

  return queue;
};

DBQueue.prototype.query = async function(sql, bindings) {
  const connection = await this.db.getConnection();
  const [results] = await connection.query(sql, bindings);

  connection.release();
  return results;
};

DBQueue.prototype.insert = async function(queue_name, data) {
  const to_store = this.serializer(data);

  const sql = `
    INSERT INTO ?? (queue, data, worker, create_time, update_time)
    VALUES (?, ?, 'unassigned', NOW(), NOW())
  `;

  return this.query(sql, [this.table, queue_name, to_store]);
};

async function reserveJobs(queue, queue_input, options) {
  const self = queue;
  const table = queue.table;
  const worker_id = uuid.v4();

  const lock_time = options.lock_time || (60 * 5);
  const limit = options.count || 1;

  const result = await self.query("SELECT NOW() AS now, NOW() + INTERVAL ? SECOND AS lock_until", [lock_time]);

  const now = result[0].now;
  const lock_until = result[0].lock_until;

  const reserve_jobs_sql = `
    UPDATE ??
    SET
      worker = ?
      , locked_until = ?
      , update_time = ?
    WHERE locked_until < ?
    AND queue IN (?)
    LIMIT ?
  `;

  const reserve_jobs_result = await self.query(reserve_jobs_sql, [table, worker_id, lock_until, now, now, queue_input, limit]);
  if (!reserve_jobs_result.affectedRows) {
    return;
  }

  const find_reserved_jobs_sql = `
    SELECT *
    FROM ??
    WHERE worker = ?
    AND locked_until = ?
  `;

  return self.query(find_reserved_jobs_sql, [table, worker_id, lock_until]);
}

DBQueue.prototype.consume = async function(queue_input, options = {}) {
  const table = this.table;
  const self = this;

  const rows = await reserveJobs(this, queue_input, options);
  if (!rows || !rows.length) {
    // not tested, but a potential race condition due to replication latency in multi-master setup
    // let's avoid an uncaught exception when we try to pull .data off of undefined
    return [];
  }

  return rows.map((job) => { return {
    data: self.deserializer(job.data),
    ack: async function () {
      try {
        await self.query("DELETE FROM ?? WHERE id = ?", [table, job.id]);
      } catch(err) {
        console.error('Error acking message:', err, err.stack);
      }
    },
    nack: async function (error) {
      if (!self.persist_last_error) {
        return;
      }

      try {
        await self.query("UPDATE ?? SET last_error = ? WHERE id = ?", [table, (error || '').toString(), job.id]);
      } catch(err) {
        console.error('Error recording last_error:', err, err.stack);
      }

      return;
    },
  }});
};

DBQueue.prototype.listen = function(queue_name, options, consumer) {
  const interval = options.interval || 1000;
  const max_outstanding = options.max_outstanding || 1;
  const max_at_a_time = options.max_jobs_per_interval || 0;
  let outstanding = 0;

  const timer = setInterval(async function() {
    let num_to_consume = max_outstanding - outstanding;
    if (!num_to_consume) {
      return;
    }

    if (max_at_a_time) {
      num_to_consume = Math.min(num_to_consume, max_at_a_time);
    }

    const consume_options = {
      lock_time: options.lock_time,
      count: num_to_consume,
    };

    let messages;
    try {
      messages = await this.consume(queue_name, consume_options);
    } catch(err) {
      return;
    }

    for (const message of messages) {
      if (!message.data) {
        return;
      }

      outstanding++;
      await consumer(
        message.data,
        async function() {
          await message.ack();
          outstanding--;
        },
        async function(err) {
          await message.nack(err);
          outstanding--;
        },
      );
    }
  }.bind(this), interval);

  function stop() {
    clearInterval(timer);
  }

  return stop;
};

DBQueue.prototype.size = async function(queue_input) {
  const table = this.table;

  const total_jobs_sql = `
    SELECT COUNT(1) AS total
    FROM ??
    WHERE queue IN (?)
  `;
  const rows = await this.query(total_jobs_sql, [table, queue_input]);

  return rows[0].total;
};

module.exports = DBQueue;
