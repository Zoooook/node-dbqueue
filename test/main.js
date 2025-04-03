'use strict';

const _ = require('lodash');
const yaml = require('js-yaml');
const uuid = require('uuid');
const helper = require('./helper.js');
const expect = helper.expect;
const DBQueue = require('../');
const db = helper.test_db;
const Promise = require('bluebird');

function withoutTimestamps(job_row) {
  return _.omit(job_row, 'create_time', 'update_time', 'id', 'locked_until');
}

describe('DBQueue', function() {
  it('can be instantiated', function() {
    new DBQueue({});
  });

  describe('.connect', function() {
    it('returns a DBQueue instance', async function() {
      const queue = await DBQueue.connect(helper.test_db_config);

      expect(queue).to.be.an.instanceof(DBQueue);
    });

    context('when the database port is invalid', function() {
      let config;

      beforeEach(async function() {
        config = _.extend({}, helper.test_db_config, {
          port: 10,
        });
      });

      it('yields an error rather than throw an exception', async function() {
        let error;

        try {
          const queue = await DBQueue.connect(config);
        } catch(err) {
          error = err;
        }

        expect(error).to.exist();

        expect(error.code).to.equal('ECONNREFUSED');
      });
    });
  });

  describe('#insert', function() {
    let queue;

    beforeEach(async function() {
      queue = await DBQueue.connect(helper.test_db_config);
    });

    it('inserts a message onto the queue', async function() {
      await queue.insert('waffles', { example:'message data' });

      const [rows] = await db.query('SELECT * FROM jobs');
      const actual = rows.map(withoutTimestamps);

      expect(actual).to.deep.equal([
        {
          data: '{"example":"message data"}',
          last_error: null,
          queue: 'waffles',
          worker: 'unassigned',
        }
      ]);
    });

    context('when called on a newly instantiated object', function() {
      let queue;

      beforeEach(function() {
        queue = new DBQueue(helper.test_db_config);
      });

      it('lazily connects to the datastore', async function() {
        await queue.insert('waffles', '{"example":"message data"}');

        const [rows] = await db.query('SELECT * FROM jobs');
        expect(rows).to.have.length(1);
      });
    });

    context('when the database port is invalid', function() {
      let config;

      beforeEach(async function() {
        config = _.extend({}, helper.test_db_config, {
          port: 10,
        });
      });

      it('yields an error rather than throw an exception', async function() {
        queue = new DBQueue(config);

        let error;

        try {
          await queue.insert('queue name', 'fake message');
        } catch(err) {
          error = err;
        }

        expect(error).to.exist();
      });
    });
  });

  describe('#consume', function() {
    let queue;

    beforeEach(async function() {
      queue = await DBQueue.connect(helper.test_db_config);
    });

    context('when there are jobs', function() {
      beforeEach(async function() {
        await Promise.all([
          queue.insert('queue_a', 'fake data for a'),
          queue.insert('queue_b', 'fake data for b'),
        ]);
      });

      it('returns a job from the queue', async function() {
        const fake_uuid = 'fakeuuid-0000-1111-2222-333333333333';
        this.sinon.stub(uuid, 'v4').returns(fake_uuid);

        const messages = await queue.consume('queue_a');
        expect(messages[0].data).to.equal('fake data for a');
      });

      it('gives a job out no more than once', async function() {
        let messages = await queue.consume('queue_a');
        expect(messages.length).to.equal(1);

        messages = await queue.consume('queue_a');
        expect(messages.length).to.equal(0);
      });

      it('leaves the job on the queue with an updated lock time', async function() {
        await queue.consume('queue_a');

        const [rows] = await db.query('SELECT * FROM jobs WHERE queue = ?', ['queue_a']);

        expect(rows).to.have.length(1);
        expect(rows[0].locked_until).to.be.afterTime(new Date());
      });

      context('and more than one queue is specified', function() {
        it('returns jobs from any of the specified queues', async function() {
          let messages = await queue.consume(['queue_a','queue_b']);
          expect(messages.length).to.equal(1);

          messages = await queue.consume(['queue_a','queue_b']);
          expect(messages.length).to.equal(1);

          messages = await queue.consume(['queue_a','queue_b']);
          expect(messages.length).to.equal(0);
        });
      });

      context('and the message is acked', function() {
        it('removes the job from the queue', async function() {
          const messages = await queue.consume('queue_a');
          await messages[0].ack();

          const [rows] = await db.query('SELECT * FROM jobs WHERE queue = ?', ['queue_a']);
          expect(rows).to.have.length(0);
        });

        context('more than once', function() {
          it('removes the job from the queue without error', async function() {
            const messages = await queue.consume('queue_a');
            await messages[0].ack();
            await messages[0].ack();

            const [rows] = await db.query('SELECT * FROM jobs WHERE queue = ?', ['queue_a']);
            expect(rows).to.deep.equal([]);
          });
        });
      });

      context('and the message is nacked', function() {
        it('leaves the job on the queue', async function() {
          const messages = await queue.consume('queue_a');
          await messages[0].nack(new Error('fake error'));

          const [rows] = await db.query('SELECT * FROM jobs WHERE queue = ?', ['queue_a']);
          expect(rows).to.have.length(1);
        });

        context('when the persist_last_error option has been specified', function() {
          beforeEach(async function() {
            const custom_config = _.extend({}, helper.test_db_config, {
              persist_last_error: true,
            });

            queue = await DBQueue.connect(custom_config);
          });

          it('should store that error in the `last_error` column', async function() {
            const messages = await queue.consume('queue_a');
            await messages[0].nack(new Error('fake error'));

            const [rows] = await db.query('SELECT last_error FROM jobs WHERE queue = ?', ['queue_a']);
            expect(rows).to.deep.equal([{ last_error: 'Error: fake error' }]);
          });
        });
      });
    });

    context('when there is more than one job', function() {
      beforeEach(async function() {
        await queue.insert('queue_a', 'first');
        await queue.insert('queue_a', 'second');
      });

      it('returns all of them one at a time', async function() {
        let [first] = await queue.consume('queue_a');
        expect(first).to.exist();

        let [second] = await queue.consume('queue_a');
        expect(second).to.exist();
        expect(second.data).to.not.equal(first.data);

        let messages = await queue.consume('queue_a');
        expect(messages.length).to.equal(0);
      });
    });

    context('when the desired queue is empty', function() {
      it('returns nothing', async function() {
        const messages = await queue.consume('queue_a');
        expect(messages.length).to.equal(0);
      });
    });

    context('when the desired queue does not exist', function() {
      it('returns nothing', async function() {
        const messages = await queue.consume('queue_c');
        expect(messages.length).to.equal(0);
      });
    });

    context('when provided an options object', function() {
      let options;

      beforeEach(async function() {
        options = {};

        await queue.insert('a queue', 'message 1');
      });

      context('that has a lock time', function() {
        beforeEach(function() {
          const hour_in_seconds = 60 * 60;
          options.lock_time = hour_in_seconds;
        });

        it('locks the jobs for that long', async function() {
          const messages = await queue.consume('a queue', options);

          const [rows] = await db.query("SELECT locked_until FROM jobs");
          expect(rows).to.have.length(1);

          const minute = 60*1000;
          const expected = new Date(Date.now() + (45 * minute));
          expect(rows[0].locked_until).to.be.afterTime(expected);
        });
      });

      context('that has a count', function() {
        beforeEach(async function() {
          options.count = 2;

          await queue.insert('a queue', 'message 2');
          await queue.insert('a queue', 'message 3');
        });

        it('returns that many messages', async function() {
          const messages = await queue.consume('a queue', options);
          expect(messages.length).to.equal(2);

          const [rows] = await db.query("SELECT locked_until FROM jobs WHERE locked_until > NOW()");
          expect(rows).to.have.length(2);
        });

        context('that is greater than the number of messages', function() {
          beforeEach(async function() {
            options.count = 3;
          });

          it('returns all available messages', async function() {
            const messages = await queue.consume('a queue', options);
            expect(messages.length).to.equal(3);
          });
        });
      });
    });
  });

  describe('#listen', function() {
    let queue;
    let listen_options;
    let consumer;

    beforeEach(async function() {
      queue = new DBQueue(helper.test_db_config);

      listen_options  = {
        interval: 1000,
        lock_time: 5,
      };

      consumer = this.sinon.spy();

      await Promise.map(['a', 'b', 'c', 'd', 'e'], (message) => {
        return queue.insert('a queue', message);
      });
    });

    it('consumes messages on an interval', async function() {
      this.sinon.stub(queue, 'consume');

      const clock = this.sinon.useFakeTimers();

      queue.listen('a queue', listen_options, consumer);
      expect(queue.consume).not.to.have.been.called();
      await clock.tickAsync(5);
      expect(queue.consume).not.to.have.been.called();
      await clock.tickAsync(1000);

      expect(queue.consume).to.have.been.calledOnce();
      await clock.tickAsync(500);
      expect(queue.consume).to.have.been.calledOnce();
      await clock.tickAsync(1000);
      expect(queue.consume).to.have.been.calledTwice();
    });

    it('can stop listening', async function() {
      this.sinon.stub(queue, 'consume');

      const clock = this.sinon.useFakeTimers();

      const stop = queue.listen('a queue', listen_options, consumer);
      console.log('stop', stop);
      await clock.tickAsync(1500);
      expect(queue.consume).to.have.been.calledOnce();
      await clock.tickAsync(1000);
      expect(queue.consume).to.have.been.calledTwice();
      stop();
      await clock.tickAsync(2000);
      expect(queue.consume).to.have.been.calledTwice();
    });

    it('passes arguments through to DBQueue#consume', async function() {
      this.sinon.stub(queue, 'consume');

      const clock = this.sinon.useFakeTimers();

      queue.listen('a queue', listen_options, consumer);
      await clock.tickAsync(1500);
      expect(queue.consume).to.have.been.calledOnce();

      const expected_options = {
        lock_time: 5,
        count: 1,
      };

      expect(queue.consume).to.have.been.calledWith('a queue', expected_options);
    });

    it('the number of messages being processed never exceeds `max_outstanding`', async function() {
      const clock = this.sinon.useFakeTimers();

      let num_processed = 0;
      function consumer(message, ack, nack) {
        setTimeout(ack, 2000 * ++num_processed);
      }

      const fakeAck = this.sinon.spy();
      let num_messages = 0;
      const max_messages = 5;
      this.sinon.stub(queue, 'consume').callsFake(async function(queue_name, consume_options) {
        if (num_messages++ >= max_messages) {
          return Promise.resolve([]);
        }
        return Promise.resolve([{
          data: 'fake message',
          ack: fakeAck,
        }]);
      });

      listen_options = {
        max_outstanding: 2,
        lock_time: 3000,
        interval: 1000,
      };
      queue.listen('a queue', listen_options, consumer);

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 2, lock_time: 3000 }]]); // called at 1 second
      queue.consume.resetHistory();

      await clock.tickAsync(100);
      expect(queue.consume).not.to.have.been.called();

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 1, lock_time: 3000 }]]); // called at 2 seconds
      queue.consume.resetHistory();

      await clock.tickAsync(1000);
      expect(fakeAck).to.have.callCount(1); // called at 1 + 2 = 3 seconds
      expect(queue.consume).not.to.have.been.called();

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 1, lock_time: 3000 }]]); // called at 4 seconds
      queue.consume.resetHistory();

      await clock.tickAsync(2000);
      expect(fakeAck).to.have.callCount(2); // called at 2 + 4 = 6 seconds
      expect(queue.consume).not.to.have.been.called();

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 1, lock_time: 3000 }]]); // called at 7 seconds
      queue.consume.resetHistory();

      await clock.tickAsync(3000);
      expect(fakeAck).to.have.callCount(3); // called at 4 + 6 = 10 seconds
      expect(queue.consume).not.to.have.been.called();

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 1, lock_time: 3000 }]]); // called at 11 seconds
      queue.consume.resetHistory();

      await clock.tickAsync(4000);
      expect(fakeAck).to.have.callCount(4); // called at 7 + 8 = 15 seconds
      expect(queue.consume).not.to.have.been.called();

      await clock.tickAsync(1000);
      expect(queue.consume.args).to.deep.equal([['a queue', { count: 1, lock_time: 3000 }]]); // called at 16 seconds
      queue.consume.resetHistory();

      await clock.tickAsync(5000);
      expect(fakeAck).to.have.callCount(5); // called at 11 + 10 = 21 seconds
      expect(queue.consume.args).to.deep.equal([
        ['a queue', { count: 1, lock_time: 3000 }],
        ['a queue', { count: 1, lock_time: 3000 }],
        ['a queue', { count: 1, lock_time: 3000 }],
        ['a queue', { count: 1, lock_time: 3000 }],
        ['a queue', { count: 1, lock_time: 3000 }],
      ]); // called every second but no more messages to consume
      queue.consume.resetHistory();

      await clock.tickAsync(3000);
      expect(queue.consume.args).to.deep.equal([
        ['a queue', { count: 2, lock_time: 3000 }],
        ['a queue', { count: 2, lock_time: 3000 }],
        ['a queue', { count: 2, lock_time: 3000 }],
      ]); // called every second with count: 2
    });

    context('when provided a max_jobs_per_interval', function() {
      let listen_options;
      let interval;

      beforeEach(function() {
        interval = 500;

        listen_options = {
          max_outstanding: 4,
          max_jobs_per_interval: 3,
          lock_time: 60000,
          interval: interval,
        };
      });

      it('will fetch only up to that many jobs at a time', async function() {
        this.sinon.stub(queue, 'consume');

        const consumer = this.sinon.spy();

        const clock = this.sinon.useFakeTimers();

        queue.listen('a queue', listen_options, consumer);

        expect(queue.consume).to.have.callCount(0);

        await clock.tickAsync(interval + 10);

        expect(queue.consume).to.have.callCount(1);
        expect(queue.consume.args[0][1]).to.deep.equal({
          count: 3,
          lock_time: 60000,
        });
      });
    });

    context('when options.interval is provided', function() {
      it('checks for new messages every <interval> milliseconds', async function() {
        const clock = this.sinon.useFakeTimers();
        const consumer = this.sinon.spy();
        const interval = 100;

        this.sinon.spy(queue, 'consume');

        listen_options = {
          max_outstanding: 100,
          lock_time: 3000,
          interval: interval,
        };

        queue.listen('a queue', listen_options, consumer);

        expect(queue.consume).to.have.callCount(0);

        await clock.tickAsync(12*interval + 10);

        expect(queue.consume).to.have.callCount(12);
      });
    });

    context('when there are no messages', function() {
      it('does not call the consumer', async function() {
        const clock = this.sinon.useFakeTimers();
        const consumer = this.sinon.spy();

        this.sinon.stub(queue, 'consume').callsFake(async function(queue_name, consume_options) {
          return [];
        });

        listen_options = {
          max_outstanding: 2,
          lock_time: 3000,
          interval: 100,
        };

        queue.listen('an empty queue', listen_options, consumer);

        await clock.tickAsync(30000);

        expect(consumer).to.have.callCount(0);
      });
    });
  });

  describe('#size', function() {
    let queue;

    beforeEach(async function() {
      queue = await DBQueue.connect(helper.test_db_config);
    });

    context('when there aren\'t any jobs', function() {
      it('returns 0', async function() {
        const count = await queue.size('queue_a');

        expect(count).to.equal(0);
      });
    });

    context('when there are jobs', function() {
      beforeEach(async function() {
        await Promise.all([
          queue.insert('queue_a', 'fake data for a'),
          queue.insert('queue_b', 'fake data for b'),
        ]);
      });

      it('returns the number of jobs in the queue', async function() {
        const count = await queue.size('queue_a');

        expect(count).to.equal(1);
      });

      context('when multiple queues are requested', function() {
        it('returns the total number of jobs across the queues', async function() {
          const count = await queue.size(['queue_a', 'queue_b']);

          expect(count).to.equal(2);
        });
      });
    });
  });

  describe('integration tests', function() {
    describe('custom table name support', function() {
      let queue;

      beforeEach(async function() {
        const custom_config = _.extend({}, helper.test_db_config, {
          table_name: 'custom_jobs_table',
        });

        queue = await DBQueue.connect(custom_config);
      });

      context('when provided a custom table name', function() {
        it('uses the provided table name', async function() {
          await queue.insert('custom_table_queue', 'fake data for custom table queue');
          const size = await queue.size('custom_table_queue');
          expect(size).to.equal(1);

          let [rows] = await db.query('SELECT * FROM jobs');
          expect(rows).to.deep.equal([]);

          [rows] = await db.query('SELECT * FROM custom_jobs_table');
          expect(rows).to.have.length(1);

          const messages = await queue.consume('custom_table_queue');
          expect(messages.length).to.equal(1);
          expect(messages[0].data).to.equal('fake data for custom table queue');

          await messages[0].ack();
          [rows] = await db.query('SELECT * FROM custom_jobs_table');
          expect(rows).to.have.length(0);
        });
      });
    });

    describe('serialization support', function() {
      context('when provided serializer/deserializer functions', function() {
        it('uses those functions to serialize/deserialize job data', async function() {
          const options = _.extend({}, helper.test_db_config, {
            serializer: yaml.dump,
            deserializer: yaml.load,
          });

          const queue = new DBQueue(options);

          await queue.insert('a queue', { fake: 'job data' });
          const [rows] = await db.query('SELECT data FROM jobs');
          expect(rows).to.deep.equal([{ data: 'fake: job data\n' }]);

          const messages = await queue.consume('a queue');
          expect(messages[0].data).to.deep.equal({ fake: 'job data' });
        });
      });

      context('when not provided a serializer/deserializer', function() {
        it('defaults to JSON.stringify/JSON.parse', async function() {
          const queue = new DBQueue(helper.test_db_config);

          await queue.insert('a queue', { fake: 'job data' });
          const [rows] = await db.query('SELECT data FROM jobs');
          expect(rows).to.deep.equal([{ data: '{"fake":"job data"}' }]);

          const messages = await queue.consume('a queue');
          expect(messages[0].data).to.deep.equal({ fake: 'job data' });
        });
      });

      context('when serialization fails', function() {
        it('yields an error', async function() {
          const options = _.extend({}, helper.test_db_config, {
            serializer: function(data) {
              return data.someInvalidMethod();
            },
          });

          const queue = new DBQueue(options);

          let err;
          try {
            await queue.insert('a queue', { fake: 'job data' });
          } catch(error) {
            err = error;
          }
          expect(err).to.exist();
        });
      });

      context('when deserialization fails', function() {
        it('yields an error', async function() {
          const options = _.extend({}, helper.test_db_config, {
            serializer: function(data) {
              return data;
            },
          });

          const queue = new DBQueue(options);

          await queue.insert('a queue', 'an invalid json string');
          const [rows] = await db.query('SELECT data FROM jobs');
          expect(rows).to.deep.equal([{ data: 'an invalid json string' }]);

          let err;
          try {
            await queue.consume('a queue');
          } catch(error) {
            err = error;
          }
          expect(err).to.exist();
        });
      });
    });
  });
});
