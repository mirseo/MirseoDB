<script>
  import { onMount } from 'svelte';
  
  let sql = '';
  let result = '';
  let loading = false;
  let authToken = '';
  let showResult = false;
  let requires2FA = false;

  const executeQuery = async () => {
    if (!sql.trim()) return;
    
    loading = true;
    showResult = false;
    requires2FA = false;
    
    try {
      const payload = {
        sql: sql.trim()
      };
      
      if (authToken.trim()) {
        payload.authtoken = authToken.trim();
      }
      
      const response = await fetch('/api/query', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify(payload)
      });
      
      const data = await response.text();
      result = JSON.stringify(JSON.parse(data), null, 2);
      
      if (data.includes('"requires_2fa":true')) {
        requires2FA = true;
      }
      
      showResult = true;
    } catch (error) {
      result = `Error: ${error.message}`;
      showResult = true;
    } finally {
      loading = false;
    }
  };

  const setup2FA = async () => {
    loading = true;
    
    try {
      const response = await fetch('/api/2fa/setup', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.text();
      result = JSON.stringify(JSON.parse(data), null, 2);
      showResult = true;
    } catch (error) {
      result = `Error: ${error.message}`;
      showResult = true;
    } finally {
      loading = false;
    }
  };

  const getQRCode = async () => {
    loading = true;
    
    try {
      const response = await fetch('/api/2fa/qr', {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json'
        }
      });
      
      const data = await response.json();
      
      if (data.qr_ascii) {
        result = data.qr_ascii;
      } else {
        result = JSON.stringify(data, null, 2);
      }
      
      showResult = true;
    } catch (error) {
      result = `Error: ${error.message}`;
      showResult = true;
    } finally {
      loading = false;
    }
  };

  const verifyToken = async () => {
    if (!authToken.trim()) return;
    
    loading = true;
    
    try {
      const response = await fetch('/api/2fa/verify', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json'
        },
        body: JSON.stringify({
          totp_token: authToken.trim()
        })
      });
      
      const data = await response.text();
      result = JSON.stringify(JSON.parse(data), null, 2);
      showResult = true;
    } catch (error) {
      result = `Error: ${error.message}`;
      showResult = true;
    } finally {
      loading = false;
    }
  };

  onMount(() => {
    // Check server health on mount
    fetch('/api/health')
      .then(response => response.json())
      .then(data => {
        console.log('MirseoDB Server Status:', data);
      })
      .catch(error => {
        console.error('Failed to connect to MirseoDB server:', error);
      });
  });
</script>

<svelte:head>
  <title>MirseoDB Console</title>
</svelte:head>

<div class="container">
  <header>
    <h1>🗄️ MirseoDB Console</h1>
    <p>Lightweight Database with AnySQL HYPERTHINKING Engine</p>
  </header>

  <div class="panels">
    <div class="panel">
      <h2>SQL Query</h2>
      <textarea 
        bind:value={sql} 
        placeholder="Enter your SQL query here...
Examples:
CREATE TABLE users (id INTEGER, name TEXT, email TEXT);
INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');
SELECT * FROM users;
DROP TABLE users; -- Requires 2FA"
        rows="8"
      ></textarea>
      
      {#if requires2FA}
        <div class="auth-section">
          <label for="authToken">🔐 2FA Token Required:</label>
          <input 
            id="authToken"
            type="text" 
            bind:value={authToken} 
            placeholder="Enter 6-digit TOTP code from your authenticator app"
            maxlength="6"
          />
        </div>
      {/if}
      
      <div class="buttons">
        <button on:click={executeQuery} disabled={loading || !sql.trim()}>
          {loading ? '⏳ Executing...' : '▶️ Execute Query'}
        </button>
        
        {#if requires2FA && authToken.trim()}
          <button on:click={verifyToken} disabled={loading}>
            🔍 Verify Token
          </button>
        {/if}
      </div>
    </div>

    <div class="panel">
      <h2>2FA Setup</h2>
      <p>For sensitive operations like DROP, ALTER, or bulk DELETE/UPDATE</p>
      
      <div class="buttons">
        <button on:click={setup2FA} disabled={loading}>
          🔑 Setup 2FA
        </button>
        <button on:click={getQRCode} disabled={loading}>
          📱 Get QR Code
        </button>
      </div>
    </div>
  </div>

  {#if showResult}
    <div class="panel result">
      <h2>Result</h2>
      <pre>{result}</pre>
    </div>
  {/if}
</div>

<style>
  .container {
    max-width: 1200px;
    margin: 0 auto;
    padding: 2rem;
  }

  header {
    text-align: center;
    margin-bottom: 2rem;
  }

  header h1 {
    color: #2563eb;
    margin-bottom: 0.5rem;
  }

  .panels {
    display: grid;
    grid-template-columns: 2fr 1fr;
    gap: 2rem;
    margin-bottom: 2rem;
  }

  .panel {
    background: #f8fafc;
    border: 1px solid #e2e8f0;
    border-radius: 8px;
    padding: 1.5rem;
  }

  .panel h2 {
    color: #374151;
    margin-bottom: 1rem;
    font-size: 1.25rem;
  }

  textarea {
    width: 100%;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    padding: 0.75rem;
    font-family: 'Courier New', monospace;
    font-size: 0.875rem;
    resize: vertical;
    margin-bottom: 1rem;
  }

  .auth-section {
    margin-bottom: 1rem;
    padding: 1rem;
    background: #fef3cd;
    border: 1px solid #fbbf24;
    border-radius: 4px;
  }

  .auth-section label {
    display: block;
    font-weight: 600;
    margin-bottom: 0.5rem;
    color: #92400e;
  }

  input[type="text"] {
    width: 100%;
    border: 1px solid #d1d5db;
    border-radius: 4px;
    padding: 0.5rem;
    font-family: monospace;
    font-size: 1.1rem;
    text-align: center;
    letter-spacing: 0.1em;
  }

  .buttons {
    display: flex;
    gap: 0.75rem;
    flex-wrap: wrap;
  }

  button {
    background: #2563eb;
    color: white;
    border: none;
    border-radius: 4px;
    padding: 0.75rem 1.5rem;
    cursor: pointer;
    font-size: 0.875rem;
    transition: background-color 0.2s;
  }

  button:hover:not(:disabled) {
    background: #1d4ed8;
  }

  button:disabled {
    background: #9ca3af;
    cursor: not-allowed;
  }

  .result {
    grid-column: 1 / -1;
  }

  .result pre {
    background: #1f2937;
    color: #f9fafb;
    padding: 1rem;
    border-radius: 4px;
    overflow-x: auto;
    font-size: 0.875rem;
    line-height: 1.5;
    white-space: pre-wrap;
  }

  @media (max-width: 768px) {
    .panels {
      grid-template-columns: 1fr;
    }
    
    .container {
      padding: 1rem;
    }
  }
</style>
