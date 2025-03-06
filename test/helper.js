'use strict';

(function doNotRunInProduction() {
  const environment = process.env.NODE_ENV || '';
  const okay_to_delete = {
    ci: true,
    test: true,
    localdev: true,
    localtest: true
  };

  if (!okay_to_delete[environment]) {
    console.error("!!!\n!!! Tests should only be run in a test environment.  Aborting.\n!!!");
    process.exit(1);
  }
})();

const fs = require('fs').promises;
const chai = require('chai');
const expect = exports.expect = chai.expect;
chai.use(require('dirty-chai'));
chai.use(require('sinon-chai'));
chai.use(require('chai-datetime'));
chai.config.includeStack = true;

require('mocha-sinon');

const mysql = require('mysql2');

exports.test_db_config = {
  host: '127.0.0.1',
  user: 'root',
  password: '',
  database: 'dbqueue_testing_db',
  connectionLimit: 10,
};

const db = exports.test_db = mysql.createPoolPromise(exports.test_db_config);

before(async function() {
  // init the DB schema
  let table_schema;

  async function createTable(table_name) {
    console.log('table_name', table_name);
    console.log('table_schema', table_schema);
    const sql = table_schema.replace('CREATE TABLE jobs', 'CREATE TABLE ' + table_name);

    console.log('sql', sql);
    return db.query(sql);
  }

  async function lazilyCreateTable(table_name) {
    const [rows] = await db.query("SHOW TABLES LIKE '" + table_name + "'");
    if (rows.length) {
      return;
    }

    if (table_schema) {
      return createTable(table_name);
    }

    const buffer = fs.readFileSync(__dirname + '/../schema.sql');
    table_schema = buffer.toString();

    return createTable(table_name);
  }

  await lazilyCreateTable('jobs');
  await lazilyCreateTable('custom_jobs_table');
});

beforeEach(async function() {
  await db.query('DELETE FROM jobs');
  await db.query('DELETE FROM custom_jobs_table');
});

/*
create table `attempt_batches` (
  `id` bigint unsigned not null auto_increment primary key,
  `payer_account_id` bigint not null,
  `proposal_id` bigint not null,
  `status` enum('pending', 'processing', 'submitted', 'failed') default 'pending',
  `create_time` timestamp default CURRENT_TIMESTAMP,
  `update_time` timestamp default '1970-01-02 00:00:00'
);
*/
