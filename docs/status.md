# Network status

This page is meant to give a quick overview of:

- Public bootstrap node health
- Peerbit network connectivity indicators (later)

## Bootstrap health

<div id="bootstrap-status">Loading…</div>

<script>
  (async () => {
    const root = document.getElementById("bootstrap-status");
    try {
      const res = await fetch("status/bootstrap.json", { cache: "no-store" });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const data = await res.json();
      const rows = (data.nodes || [])
        .map((n) => {
          const ok = n.ok ? "OK" : "DOWN";
          const detail = n.detail ? ` — ${n.detail}` : "";
          return `<tr><td><code>${n.address}</code></td><td>${ok}</td><td><code>${n.checkedAt}</code></td><td>${detail}</td></tr>`;
        })
        .join("");

      root.innerHTML = `
        <p><small>Updated <code>${data.generatedAt || "unknown"}</code></small></p>
        <table>
          <thead><tr><th>Address</th><th>Status</th><th>Checked</th><th>Detail</th></tr></thead>
          <tbody>${rows || "<tr><td colspan=\\"4\\">No data</td></tr>"}</tbody>
        </table>
      `;
    } catch (err) {
      root.innerHTML =
        "<p><strong>Status data unavailable.</strong> (bootstrap checks not configured yet)</p>";
    }
  })();
</script>

