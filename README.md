# DBQueue
A simple job queue that prioritizes infrastructural simplicity and common requirements over speed and scalability, inspired by TheSchwartz

# Usage

See usage in the tests, or see below example:

## Overview

```javascript
const DBQueue = require('dbqueue');

// use the included schema.sql to initialize the DB schema

const queue_options = {
  // node-mysql compatible DB settings
  host:       '127.0.0.1',
  port:       3306, // optional, defaults to 3306
  user:       'root',
  table_name: 'custom_jobs_table', // optional, defaults to `jobs`
  password:   '',
  database:   'dbqueue_testing_db',
};

try {
  const queue = await DBQueue.connect(queue_options);
} catch(err) {
  // likely a DB connection error
}

const job_details = {
  example: 'job data',
};

// in your producer
try {
  await queue.insert('queue_name_here', JSON.stringify(job_details));
  // job enqueued, congratulations!
} catch(err) {
  // likely a DB connection error
}

// in your consumer
const messages = await queue.consume('queue_name_here');

if (!messages.length) {
  // if there are no jobs on the queue
}

for (const message of messages) {
  const job_data = JSON.parse(message.data);

  // do something with said job data

  // then let the queue know the job has been handled
  try {
    await message.ack();
  } catch (err) {
    // job is likely still on the queue
  }

  // or leave the job on the queue
  await message.nack(some_err);
```

## Connecting

Connect asynchronously to discover connectivity issues as soon as possible:
```javascript
const queue_options = {
  // options as above
};

try {
  const queue = await DBQueue.connect(queue_options);
} catch(err) {
  // likely a DB connection error
}

// start using queue
```

Connect lazily for less boilerplate
```javascript
const queue_options = {
  // options as above
};

const queue = new DBQueue(queue_options);

// start using queue, if there is a connection problem all queries are going to fail
```

## Inserting messages into the queue

```javascript
const queue_name = 'example queue';
const message_data = { example: 'message data' };
// message_data is serialized to JSON by default

await queue.insert(queue_name, message_data);
```

## Consuming messages from the queue

Message consumption reserves the message for five minutes by default. If the message is not ACK'ed within that time, the message may be processed by another worker.

```javascript
const queue_name = 'example queue';
const messages = await queue.consume(queue_name);
// handle potential error

// messages = [
//   {
//     data: thawed JSON by default
//     ack: async function
//     nack: async function
//   },
//   ...
// ]
// messages may be an empty array if there are no available jobs on the queue
```

An optional options object can be provided with the following attributes:
* count: the number of messages to attempt to consume
* lock_time: how long to lock the messages in the queue.

```javascript
const queue_name = 'example queue';
const options = {
  count:     10,
  lock_time: 60*60, // in seconds, defaults to 300 seconds (five minutes)
};

const messages = await queue.consume(queue_name, options);
```

### ACK'ing and NACK'ing messages

Calling message.ack() will remove it from the queue.

Calling message.nack() (with an optional error) will leave it on the queue to be processed again after some time.

Not calling message.ack() will leave it on the queue to be processed again after some time.

```javascript
const queue_name = 'example queue';
const messages = await queue.consume(queue_name);
// handle potential error

for (const message of messages) {
  try {
    await doSomethingWithMessage(message.data);
    await message.ack();
  } catch(err) {
    await message.nack(err);
  }
}
```

## Listening to the queue

```javascript
const queue_name = 'default_queue_configuration';
const options = {
  interval:           1000, // milliseconds to wait between polling the queue, defaults to 1000
  max_outstanding:       1, // maximum un-ack'ed outstanding messages to have, defaults to 1
  max_jobs_per_interval: 0, // maximum number of messages to consume per interval, defaults to 0
                            // if set to 0, there is no limit per-interval, but max_outstanding is still enforced
};

async function consumer(message_data, ack, nack) {
  console.log("message:", message_data);

  await message.ack();
}

queue.listen(queue_name, options, consumer);
```

## Example rate-limited consumer for slow jobs
Consume at a steady rate of ~4 messages/sec, up to 10,000 jobs in flight.

```javascript
const queue_name = 'slow_job_queue_with_high_concurrency';
const options = {
  interval:              500,   // check for jobs twice a second
  max_jobs_per_interval: 2,
  max_outstanding:       10000,
  lock_time:             10*60, // jobs take a while, so lock for longer
};

async function consumer(message_data, ack, nack) {
  // the same signature as the `consume` handler above
}

queue.listen(queue_name, options, consumer);
```

## Custom serialization

In case you would like something other than JSON.stringify and JSON.parse for serialization, provide your own serialization methods.

Note that binary formats are currently not supported.

```javascript
const yaml = require('js-yaml');

const queue_options = {
  // ... options as before
  serializer:   yaml.dump,
  deserializer: yaml.load,
};

const queue = new DBQueue(queue_options);
```

# When this might be a useful library
* You don't want to introduce another dependency for simple/trivial functionality
* You need a simple, durable queue
* You are okay with at least once semantics
* You would like message deferral without dead letter queue complexity

# When this is NOT the solution for you
* You need guarantees that a job will be delivered once and _only_ once (your jobs are not idempotent)
* You need near-realtime performance
* You need to scale to large numbers of jobs and/or very high throughput

# Performance improvements
* fetch batches of jobs rather than one at a time
  * when #pop is called
    * and we have no items in the working batch
      * look for N jobs to work on
      * reserve them all
      * shift the first off and return it
    * and we *do* have items in the working batch
      * shift the first off and return it
      * reserve another N ?
  * so long as we can process the jobs quickly, this should be okay
    * but if we're too slow, we might have stale jobs that someone else is working on
