const jobsBody = document.getElementById("jobs");
const statusEl = document.getElementById("status");

function formatTime(iso) {
  if (!iso || iso.startsWith("0001")) return "—";
  const d = new Date(iso);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleString();
}

function render(jobs) {
  jobsBody.innerHTML = "";
  if (!jobs.length) {
    const row = document.createElement("tr");
    row.innerHTML = '<td colspan="8">No jobs registered.</td>';
    jobsBody.appendChild(row);
    return;
  }

  for (const job of jobs) {
    const row = document.createElement("tr");
    const leader = job.leaderRun
      ? '<span class="badge leader">leader</span>'
      : '<span class="badge follower">follower</span>';
    row.innerHTML = `
      <td>${job.id}</td>
      <td>${job.name || "—"}</td>
      <td><code>${job.spec}</code></td>
      <td>${leader}</td>
      <td>${job.lastStatus || "unknown"}</td>
      <td>${formatTime(job.lastRun)}</td>
      <td>${formatTime(job.nextRun)}</td>
      <td class="error">${job.lastError || ""}</td>
    `;
    jobsBody.appendChild(row);
  }
}

async function refresh() {
  try {
    const res = await fetch("api/jobs");
    if (!res.ok) throw new Error(res.statusText);
    const data = await res.json();
    render(data.jobs || []);
    statusEl.hidden = true;
  } catch (err) {
    statusEl.hidden = false;
    statusEl.textContent = `Failed to load jobs: ${err.message}`;
  }
}

refresh();
setInterval(refresh, 5000);
