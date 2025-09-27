#!/usr/bin/env node

const http = require('http');

const DEFAULT_PORT = parseInt(process.env.MIRSEO_HEALTH_PORT || '3306', 10);
const DEFAULT_PATH = process.env.MIRSEO_HEALTH_PATH || '/health';

function runHealthCheck(port, path) {
  const options = {
    hostname: '127.0.0.1',
    port,
    path,
    method: 'GET',
    timeout: 2000,
  };

  const req = http.request(options, (res) => {
    let body = '';

    res.on('data', (chunk) => {
      body += chunk;
    });

    res.on('end', () => {
      console.log(`[health.js] ${res.statusCode} ${res.statusMessage}`);
      if (body.length > 0) {
        console.log(`[health.js] Body: ${body}`);
      }
    });
  });

  req.on('error', (err) => {
    console.error(`[health.js] Health check failed: ${err.message}`);
  });

  req.on('timeout', () => {
    req.destroy(new Error('Request timed out'));
  });

  req.end();
}

runHealthCheck(DEFAULT_PORT, DEFAULT_PATH);
