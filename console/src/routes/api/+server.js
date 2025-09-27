// API proxy to MirseoDB server
const MIRSEODB_SERVER = 'http://localhost:3306';

async function proxyRequest(url, options = {}) {
  try {
    const response = await fetch(`${MIRSEODB_SERVER}${url}`, options);
    const data = await response.text();
    
    return new Response(data, {
      status: response.status,
      headers: {
        'Content-Type': response.headers.get('content-type') || 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization'
      }
    });
  } catch (error) {
    return new Response(JSON.stringify({
      error: `Failed to connect to MirseoDB server: ${error.message}`,
      server: MIRSEODB_SERVER
    }), {
      status: 503,
      headers: {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*'
      }
    });
  }
}

export async function GET({ url }) {
  const pathname = url.pathname.replace('/api', '');
  return proxyRequest(pathname);
}

export async function POST({ request, url }) {
  const pathname = url.pathname.replace('/api', '');
  const body = await request.text();
  
  return proxyRequest(pathname, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    body
  });
}

export async function OPTIONS() {
  return new Response(null, {
    status: 200,
    headers: {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization'
    }
  });
}