<svelte:head>
  <title>MirseoDB Console</title>
</svelte:head>

<main class="page">
  <section class="panel">
    <h1>MirseoDB Console</h1>
    <p>
      Welcome to the experimental MirseoDB web console. This placeholder is built with Svelte 5
      and will evolve into a Supabase-style admin experience.
    </p>
    <p>
      You can start editing <code>console/src/routes/+page.svelte</code> to build out the console UI.
    </p>
  </section>
</main>

<style>
  :global(body) {
    font-family: system-ui, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif;
    margin: 0;
    background: #0f111a;
    color: #f8fafc;
  }

  .page {
    display: flex;
    min-height: 100vh;
    align-items: center;
    justify-content: center;
    padding: 2rem;
    background: radial-gradient(circle at top, rgba(59, 130, 246, 0.15), transparent 60%),
      radial-gradient(circle at bottom, rgba(139, 92, 246, 0.2), transparent 55%),
      linear-gradient(160deg, #111827, #0b1120 60%);
  }

  .panel {
    border-radius: 1.5rem;
    background: rgba(15, 23, 42, 0.85);
    box-shadow: 0 20px 60px rgba(15, 23, 42, 0.35);
    backdrop-filter: blur(18px);
    padding: 2.5rem;
    max-width: 720px;
  }

  h1 {
    font-size: clamp(2.2rem, 4vw, 3rem);
    margin-bottom: 1rem;
  }

  p {
    line-height: 1.7;
    margin: 0.75rem 0;
  }

  code {
    font-family: 'JetBrains Mono', 'Fira Code', 'SFMono-Regular', ui-monospace, monospace;
    background: rgba(148, 163, 184, 0.1);
    padding: 0.2rem 0.4rem;
    border-radius: 0.4rem;
  }
</style>
