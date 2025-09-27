#!/usr/bin/env node

const http = require('http');
const net = require('net');

const HOST = process.env.MIRSEO_HOST || '127.0.0.1';
const PORT = Number(process.env.MIRSEO_PORT || 3306);
const AUTH_TOKEN = process.env.MIRSEODB_API_TOKEN || '';

function logSection(title) {
  const line = '-'.repeat(title.length + 4);
  console.log(`\n${line}\n> ${title}\n${line}`);
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

function rawRequestWithSplitHeader(sql) {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: HOST, port: PORT }, () => {
      const body = sql;
      const headers = [
        'POST /query HTTP/1.1',
        `Host: ${HOST}`,
        'Content- Type: application/sql',
        `Content-Length: ${Buffer.byteLength(body)}`,
      ];

      if (AUTH_TOKEN) {
        headers.push(`Authorization: Bearer ${AUTH_TOKEN}`);
      }

      const request = `${headers.join('\r\n')}\r\n\r\n${body}`;
      socket.write(request);
    });

    let data = '';
    socket.on('data', (chunk) => {
      data += chunk;
    });

    socket.on('end', () => resolve(data));
    socket.on('error', reject);
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

async function sendSql(sql) {
  return httpRequest({
    headers: { 'Content-Type': 'application/sql' },
    body: sql,
  });
}

async function main() {
  logSection('CREATE TABLE user');
  let response = await sendJson('CREATE TABLE user (id INTEGER, email TEXT, pw TEXT);');
  console.log(response);

  logSection('INSERT row #1 (JSON)');
  response = await sendJson("INSERT INTO user (id, email, pw) VALUES (1, 'alice@example.com', 'secret');");
  console.log(response);

  logSection('INSERT row #2 (application/sql)');
  response = await sendSql("INSERT INTO user (id, email, pw) VALUES (2, 'bob@example.com', 'hunter2');");
  console.log(response);

  logSection('SELECT * FROM user');
  response = await sendJson('SELECT * FROM user;');
  console.log(response);

  logSection('Raw socket request with split Content-Type header');
  const rawResponse = await rawRequestWithSplitHeader(
    "INSERT INTO user (id, email, pw) VALUES (3, 'carol@example.com', 'pa55w0rd');"
  );
  console.log(rawResponse);

  logSection('SELECT * FROM user after raw request');
  response = await sendJson('SELECT * FROM user;');
  console.log(response);
}

main().catch((err) => {
  console.error('[test.js] Error:', err.message || err);
  process.exitCode = 1;
});
