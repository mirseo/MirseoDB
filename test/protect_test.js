#!/usr/bin/env node

const http = require('http');

const HOST = process.env.MIRSEO_HOST || '127.0.0.1';
const PORT = Number(process.env.MIRSEO_PORT || 3306);
const AUTH_TOKEN = process.env.MIRSEODB_API_TOKEN || '';

function logSection(title) {
  const line = '-'.repeat(title.length + 4);
  console.log(`\n${line}\n> ${title}\n${line}`);
}

function assert(condition, message) {
  if (!condition) {
    console.error(`\n[protect_test] Assertion failed: ${message}`);
    process.exitCode = 1;
  }
}

function httpRequest({ method = 'POST', path = '/query', headers = {}, body = '' }) {
  return new Promise((resolve, reject) => {
    const options = {
      host: HOST,
      port: PORT,
      method,
      path,
      headers: {
        'Content-Length': Buffer.byteLength(body),
        ...headers,
      },
    };

    if (AUTH_TOKEN && !('Authorization' in options.headers)) {
      options.headers.Authorization = `Bearer ${AUTH_TOKEN}`;
    }

    const req = http.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => {
        data += chunk;
      });
      res.on('end', () => {
        resolve({ statusCode: res.statusCode, statusMessage: res.statusMessage, body: data });
      });
    });

    req.on('error', reject);
    req.write(body);
    req.end();
  });
}

async function sendJson(sql) {
  const payload = AUTH_TOKEN
    ? JSON.stringify({ sql, auth_token: AUTH_TOKEN })
    : JSON.stringify({ sql });

  return httpRequest({
    headers: { 'Content-Type': 'application/json' },
    body: payload,
  });
}

function parseBody(response) {
  try {
    return JSON.parse(response.body);
  } catch (err) {
    console.error('[protect_test] Failed to parse JSON body:', response.body);
    throw err;
  }
}

async function main() {
  const tableName = `protect_${Date.now()}`;

  logSection(`Create table ${tableName}`);
  const createResp = await sendJson(`CREATE TABLE ${tableName} (id INTEGER, note TEXT);`);
  console.log(createResp);
  assert(createResp.statusCode === 200 || createResp.statusCode === 400, 'Unexpected status for CREATE TABLE');

  logSection('Insert baseline row');
  const insertResp = await sendJson(
    `INSERT INTO ${tableName} (id, note) VALUES (1, 'safe row');`
  );
  console.log(insertResp);
  assert(insertResp.statusCode === 200, 'Expected successful INSERT');

  logSection('Baseline SELECT without injection');
  const cleanResp = await sendJson(`SELECT * FROM ${tableName} WHERE note = 'safe row';`);
  console.log(cleanResp);
  assert(cleanResp.statusCode === 200, 'Expected 200 for baseline SELECT');
  const cleanJson = parseBody(cleanResp);
  assert(!('sanitized' in cleanJson), 'Baseline query should not be sanitized');

  logSection('Injection attempt SELECT with OR 1=1');
  const injectionSql = `SELECT * FROM ${tableName} WHERE note = 'safe row' OR '1'='1';`;
  const injectionResp = await sendJson(injectionSql);
  console.log(injectionResp);
  assert(injectionResp.statusCode === 200, 'Injection attempt should still succeed after sanitization');
  const injectionJson = parseBody(injectionResp);
  assert(injectionJson.sanitized === true, 'Injection attempt should be marked sanitized');
  assert(Array.isArray(injectionJson.rows), 'Expected rows array in response');

  if (process.exitCode && process.exitCode !== 0) {
    console.error('\n[protect_test] One or more assertions failed.');
  } else {
    console.log('\n[protect_test] All checks passed.');
  }
}

main().catch((err) => {
  console.error('[protect_test] Error:', err.message || err);
  process.exitCode = 1;
});
