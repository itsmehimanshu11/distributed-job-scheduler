const API_URL = "http://localhost:8000";
const API_KEY = "BxOEw4OW6eQmHefRV3YO1a4ijWBUeJv0EJMvj4ulReE";

let allJobs = [];
let allWorkers = [];
let isCreating = false;

let currentWorkerCount = 1;
let selectedWorkerCount = 1;
let isScalingWorkers = false;

let selectedWorkers = new Set();
let isDeletingWorker = false;

let createMode = "single";

/* =========================================================
   API
========================================================= */

async function apiRequest(url, options = {}) {
    const headers = { ...(options.headers || {}) };
    headers["X-API-Key"] = API_KEY;

    const response = await fetch(`${API_URL}${url}`, { ...options, headers });

    if (!response.ok) {
        let message = `HTTP ${response.status}`;
        try {
            const data = await response.json();
            if (data.detail) message = data.detail;
        } catch (_) {}
        throw new Error(message);
    }

    return response.json();
}

/* =========================================================
   LOAD JOBS / WORKERS
========================================================= */

async function loadJobs() {
    try {
        const jobs = await apiRequest("/jobs");
        allJobs = Array.isArray(jobs) ? jobs : [];
        setConnection(true);
        updateDashboard();
    } catch (error) {
        console.error("Failed to load jobs:", error);
        setConnection(false);
    }
}

async function loadWorkers() {
    const hadPendingChange = selectedWorkerCount !== currentWorkerCount;

    try {
        if (!hadPendingChange) {
            setCapacityStatusText("Reading current worker count…");
        }

        const data = await apiRequest("/workers");

        if (data && typeof data.count === "number") {
            currentWorkerCount = data.count;
            allWorkers = Array.isArray(data.workers) ? data.workers : [];

            if (!hadPendingChange) {
                selectedWorkerCount = data.count;
            }

            const existingIds = new Set(allWorkers.map(w => String(w.id)));
            selectedWorkers = new Set([...selectedWorkers].filter(id => existingIds.has(id)));

            updateCapacityUI();
            markCapacityStatus();
            renderFleet();
            renderThroughput();
        }
    } catch (error) {
        console.error("Failed to load workers:", error);
        setCapacityStatusText(error.message || "Unable to read worker count", true);
        updateCapacityUI();
    }
}

function setConnection(online) {
    const dot = document.getElementById("connectionDot");
    const text = document.getElementById("connectionText");
    if (!dot || !text) return;

    dot.classList.remove("online", "offline");
    if (online) {
        dot.classList.add("online");
        text.textContent = "Connected";
    } else {
        dot.classList.add("offline");
        text.textContent = "Offline";
    }
}

/* =========================================================
   DASHBOARD
========================================================= */

function updateDashboard() {
    const total = allJobs.length;
    const pending = allJobs.filter(j => j.status === "pending").length;
    const running = allJobs.filter(j => j.status === "running").length;
    const completed = allJobs.filter(j => j.status === "completed").length;
    const failed = allJobs.filter(j => j.status === "failed").length;

    setText("totalJobs", total);
    setText("pendingJobs", pending);
    setText("runningJobs", running);
    setText("completedJobs", completed);
    setText("failedJobs", failed);

    renderFleet();
    renderJobs();
    renderThroughput();
}

/* =========================================================
   THROUGHPUT GRAPH
========================================================= */

function renderThroughput() {
    const line = document.getElementById("throughputLine");
    const fill = document.getElementById("throughputFill");
    const avgLabel = document.getElementById("throughputAvg");

    if (!line || !fill || !avgLabel) return;

    const windowMinutes = 30;
    const bucketMinutes = 2;
    const bucketCount = windowMinutes / bucketMinutes;

    const now = Date.now();
    const buckets = new Array(bucketCount).fill(0);

    let totalInWindow = 0;

    allJobs.forEach(job => {
        if (job.status !== "completed") return;

        const timestamp = job.claimed_at || job.created_at;
        if (!timestamp) return;

        const time = new Date(timestamp).getTime();
        if (!Number.isFinite(time)) return;

        const minutesAgo = (now - time) / 60000;
        if (minutesAgo < 0 || minutesAgo > windowMinutes) return;

        const bucketIndex = bucketCount - 1 - Math.floor(minutesAgo / bucketMinutes);
        if (bucketIndex >= 0 && bucketIndex < bucketCount) {
            buckets[bucketIndex]++;
            totalInWindow++;
        }
    });

    const maxCount = Math.max(...buckets, 1);
    const width = 760;
    const height = 60;
    const stepX = width / (bucketCount - 1);

    const points = buckets.map((count, index) => {
        const x = Math.round(index * stepX);
        const y = Math.round(height - 5 - (count / maxCount) * 45);
        return `${x},${y}`;
    });

    line.setAttribute("points", points.join(" "));
    fill.setAttribute(
        "points",
        `${points.join(" ")} ${width},${height} 0,${height}`
    );

    const avgPerMinute = (totalInWindow / windowMinutes).toFixed(1);
    avgLabel.textContent = `${avgPerMinute} jobs/min avg`;
}

/* =========================================================
   WORKER FLEET TABLE
========================================================= */

function findWorkerStats(workerMap, worker) {
    const exact = workerMap[String(worker.name)] || workerMap[String(worker.id)];
    if (exact) return exact;

    const idPrefix = `${worker.id}-`;
    const namePrefix = `${worker.name}-`;
    const key = Object.keys(workerMap).find(
        k => k.startsWith(idPrefix) || k.startsWith(namePrefix)
    );

    return key ? workerMap[key] : { total: 0, running: 0, completed: 0, failed: 0 };
}

let lastFleetSignature = "";

function renderFleet() {
    const tbody = document.getElementById("fleetTable");
    if (!tbody) return;

    const workerMap = {};
    allJobs.forEach(job => {
        if (!job.worker_id) return;
        const id = String(job.worker_id);
        if (!workerMap[id]) {
            workerMap[id] = { total: 0, running: 0, completed: 0, failed: 0 };
        }
        const w = workerMap[id];
        w.total++;
        if (job.status === "running") w.running++;
        if (job.status === "completed") w.completed++;
        if (job.status === "failed") w.failed++;
    });

    const workers = Array.isArray(allWorkers) ? allWorkers : [];

    if (!workers.length) {
        tbody.innerHTML = `<tr><td colspan="5" class="empty-cell">No active workers. Scale up above to create one.</td></tr>`;
        lastFleetSignature = "";
        updateDeleteButton();
        return;
    }

    const signature = JSON.stringify(
        workers
            .map(w => {
                const s = findWorkerStats(workerMap, w);
                return [w.id, w.name, w.status, s.total, s.completed, s.failed, selectedWorkers.has(String(w.id))];
            })
            .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    );

    if (signature === lastFleetSignature) return;
    lastFleetSignature = signature;

    tbody.innerHTML = workers
        .map(worker => {
            const stats = findWorkerStats(workerMap, worker);
            const executing = stats.running > 0;
            const successRate = stats.total > 0
                ? Math.round((stats.completed / stats.total) * 100)
                : null;
            const workerId = String(worker.id);
            const checked = selectedWorkers.has(workerId);

            return `
                <tr>
                    <td class="col-check">
                        <input type="checkbox" class="row-check worker-checkbox" data-worker-id="${escapeHtml(workerId)}" ${checked ? "checked" : ""}>
                    </td>
                    <td class="mono">${escapeHtml(worker.name)}<br><span style="color:var(--muted-2);font-size:10px;">${escapeHtml(workerId)}</span></td>
                    <td>
                        <span class="fleet-led ${executing ? "executing" : ""}"></span>
                        <span class="status-word ${executing ? "executing" : "ready"}">${executing ? "Executing" : "Ready"}</span>
                    </td>
                    <td class="col-num">${stats.total}</td>
                    <td class="col-num">${successRate === null ? "—" : `${successRate}%`}</td>
                </tr>
            `;
        })
        .join("");

    tbody.querySelectorAll(".worker-checkbox").forEach(checkbox => {
        checkbox.addEventListener("change", event => {
            const id = String(event.target.dataset.workerId);
            if (event.target.checked) {
                selectedWorkers.add(id);
            } else {
                selectedWorkers.delete(id);
            }
            updateDeleteButton();
        });
    });

    updateDeleteButton();
}

function initializeFleetSelection() {
    const selectAll = document.getElementById("selectAllWorkers");
    const deleteButton = document.getElementById("deleteSelectedWorkersButton");

    if (selectAll && !selectAll.dataset.bound) {
        selectAll.dataset.bound = "true";
        selectAll.addEventListener("change", () => {
            const checkboxes = document.querySelectorAll(".worker-checkbox");
            checkboxes.forEach(checkbox => {
                checkbox.checked = selectAll.checked;
                const id = String(checkbox.dataset.workerId);
                if (selectAll.checked) {
                    selectedWorkers.add(id);
                } else {
                    selectedWorkers.delete(id);
                }
            });
            updateDeleteButton();
        });
    }

    if (deleteButton && !deleteButton.dataset.bound) {
        deleteButton.dataset.bound = "true";
        deleteButton.addEventListener("click", deleteSelectedWorkers);
    }

    updateDeleteButton();
}

function updateDeleteButton() {
    const countLabel = document.getElementById("selectedWorkerCount");
    const deleteButton = document.getElementById("deleteSelectedWorkersButton");
    const selectAll = document.getElementById("selectAllWorkers");

    const count = selectedWorkers.size;

    if (countLabel) countLabel.textContent = count;
    if (deleteButton) deleteButton.disabled = count === 0 || isDeletingWorker;

    if (selectAll) {
        const total = document.querySelectorAll(".worker-checkbox").length;
        selectAll.checked = total > 0 && count === total;
        selectAll.indeterminate = count > 0 && count < total;
    }
}

async function deleteSelectedWorkers() {
    if (isDeletingWorker) return;

    const workers = [...selectedWorkers];
    if (!workers.length) {
        showToast("Select at least one worker");
        return;
    }

    if (workers.length >= currentWorkerCount) {
        showToast("Keep at least one worker active");
        return;
    }

    const confirmed = confirm(
        `Delete ${workers.length} selected worker${workers.length === 1 ? "" : "s"}?\n\n` +
        "This stops and removes the Docker worker container(s)."
    );
    if (!confirmed) return;

    isDeletingWorker = true;

    const deleteButton = document.getElementById("deleteSelectedWorkersButton");
    const originalText = deleteButton ? deleteButton.innerHTML : "";
    if (deleteButton) {
        deleteButton.disabled = true;
        deleteButton.textContent = "Deleting…";
    }

    let deleted = 0;
    let failed = 0;

    for (const workerId of workers) {
        try {
            await apiRequest(`/workers/${encodeURIComponent(workerId)}`, { method: "DELETE" });
            selectedWorkers.delete(workerId);
            deleted++;
        } catch (error) {
            console.error(`Failed to delete worker ${workerId}:`, error);
            failed++;
        }
    }

    isDeletingWorker = false;
    if (deleteButton) deleteButton.innerHTML = originalText;

    if (failed === 0) {
        showToast(`Deleted ${deleted} worker${deleted === 1 ? "" : "s"}`);
    } else {
        showToast(`Deleted ${deleted}; ${failed} failed`);
    }

    await Promise.all([loadJobs(), loadWorkers()]);
    updateDeleteButton();
}

/* =========================================================
   CAPACITY CONTROL
========================================================= */

function initializeCapacityControls() {
    const minusButton = document.getElementById("workerMinus");
    const plusButton = document.getElementById("workerPlus");
    const applyButton = document.getElementById("applyWorkers");
    const input = document.getElementById("desiredWorkerCount");

    if (minusButton && !minusButton.dataset.bound) {
        minusButton.dataset.bound = "true";
        minusButton.addEventListener("click", () => {
            selectedWorkerCount = Math.max(1, selectedWorkerCount - 1);
            updateCapacityUI();
            markCapacityStatus();
        });
    }

    if (plusButton && !plusButton.dataset.bound) {
        plusButton.dataset.bound = "true";
        plusButton.addEventListener("click", () => {
            selectedWorkerCount = Math.min(32, selectedWorkerCount + 1);
            updateCapacityUI();
            markCapacityStatus();
        });
    }

    if (applyButton && !applyButton.dataset.bound) {
        applyButton.dataset.bound = "true";
        applyButton.addEventListener("click", scaleWorkers);
    }

    document.querySelectorAll("[data-workers]").forEach(button => {
        if (button.dataset.bound) return;
        button.dataset.bound = "true";
        button.addEventListener("click", () => {
            const count = Number(button.dataset.workers);
            if (!Number.isInteger(count)) return;
            selectedWorkerCount = Math.min(32, Math.max(1, count));
            updateCapacityUI();
            markCapacityStatus();
        });
    });

    if (input && !input.dataset.bound) {
        input.dataset.bound = "true";

        input.addEventListener("input", () => {
            const raw = Number(input.value);
            if (Number.isFinite(raw)) {
                selectedWorkerCount = raw;
                markCapacityStatus();
            }
        });

        const commit = () => {
            let count = Number(input.value);
            if (!Number.isInteger(count) || count < 1) count = 1;
            else if (count > 32) count = 32;
            selectedWorkerCount = count;
            updateCapacityUI();
            markCapacityStatus();
        };

        input.addEventListener("blur", commit);
        input.addEventListener("keydown", event => {
            if (event.key === "Enter") {
                event.preventDefault();
                commit();
                input.blur();
            }
        });
    }

    updateCapacityUI();
}

function updateCapacityUI() {
    const input = document.getElementById("desiredWorkerCount");
    if (input && document.activeElement !== input) {
        input.value = selectedWorkerCount;
    }

    const minusButton = document.getElementById("workerMinus");
    if (minusButton) minusButton.disabled = selectedWorkerCount <= 1;

    const plusButton = document.getElementById("workerPlus");
    if (plusButton) plusButton.disabled = selectedWorkerCount >= 32;

    document.querySelectorAll("[data-workers]").forEach(button => {
        const count = Number(button.dataset.workers);
        button.classList.toggle("active", count === selectedWorkerCount);
    });
}

function markCapacityStatus() {
    if (selectedWorkerCount === currentWorkerCount) {
        setCapacityStatusText(`${currentWorkerCount} worker${currentWorkerCount === 1 ? "" : "s"} active`);
    } else {
        setCapacityStatusText(`Change pending: ${currentWorkerCount} → ${selectedWorkerCount}`);
    }
}

function setCapacityStatusText(text, isError = false) {
    const status = document.getElementById("workerScalingStatus");
    if (!status) return;
    status.innerHTML = `<span class="status-dot"></span><span>${escapeHtml(text)}</span>`;
    status.classList.toggle("error", Boolean(isError));
}

async function scaleWorkers() {
    if (isScalingWorkers) return;

    if (selectedWorkerCount < 1 || selectedWorkerCount > 32) {
        showToast("Worker count must be between 1 and 32");
        return;
    }

    if (selectedWorkerCount === currentWorkerCount) {
        showToast(`Already running ${currentWorkerCount} worker${currentWorkerCount === 1 ? "" : "s"}`);
        return;
    }

    isScalingWorkers = true;

    const applyButton = document.getElementById("applyWorkers");
    const originalText = applyButton ? applyButton.textContent : "";
    if (applyButton) {
        applyButton.disabled = true;
        applyButton.textContent = "Scaling…";
    }

    setCapacityStatusText(`Scaling to ${selectedWorkerCount} workers…`);

    try {
        const result = await apiRequest("/workers/scale", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ workers: selectedWorkerCount })
        });

        if (result && typeof result.count === "number") {
            currentWorkerCount = result.count;
            selectedWorkerCount = result.count;
        } else {
            await new Promise(resolve => setTimeout(resolve, 1500));
            await loadWorkers();
        }

        updateCapacityUI();
        markCapacityStatus();
        showToast(`Worker capacity set to ${currentWorkerCount}`);
    } catch (error) {
        console.error("Worker scaling failed:", error);
        setCapacityStatusText(error.message, true);
        showToast(`Scaling failed: ${error.message}`);
        selectedWorkerCount = currentWorkerCount;
        updateCapacityUI();
    } finally {
        isScalingWorkers = false;
        if (applyButton) {
            applyButton.disabled = false;
            applyButton.textContent = originalText || "Apply";
        }
    }
}

/* =========================================================
   JOB QUEUE TABLE
========================================================= */

function renderJobWorkerCell(workerId) {
    if (!workerId) return `<span class="mono">—</span>`;

    const isLive = Array.isArray(allWorkers) && allWorkers.some(worker => {
        const liveId = String(worker.id);
        const liveName = String(worker.name);
        return (
            workerId === liveId ||
            workerId === liveName ||
            String(workerId).startsWith(`${liveId}-`) ||
            String(workerId).startsWith(`${liveName}-`)
        );
    });

    if (isLive) return `<span class="mono">${escapeHtml(workerId)}</span>`;

    return `<span class="mono">${escapeHtml(workerId)}</span><span class="worker-offline-tag">OFFLINE</span>`;
}

function renderJobs() {
    const table = document.getElementById("jobsTable");
    if (!table) return;

    const searchInput = document.getElementById("searchInput");
    const statusFilter = document.getElementById("statusFilter");

    const search = searchInput ? searchInput.value.trim().toLowerCase() : "";
    const filter = statusFilter ? statusFilter.value : "all";

    let jobs = [...allJobs];

    if (search) {
        jobs = jobs.filter(job =>
            String(job.id).toLowerCase().includes(search) ||
            String(job.name).toLowerCase().includes(search) ||
            String(job.worker_id || "").toLowerCase().includes(search)
        );
    }

    if (filter !== "all") {
        jobs = jobs.filter(job => job.status === filter);
    }

    jobs.sort((a, b) => Number(b.id) - Number(a.id));

    if (!jobs.length) {
        table.innerHTML = `<tr><td colspan="7" class="empty-cell">No jobs found</td></tr>`;
        return;
    }

    table.innerHTML = jobs
        .map(job => {
            const status = job.status || "unknown";
            return `
                <tr>
                    <td class="mono">#${job.id}</td>
                    <td>${escapeHtml(job.name)}</td>
                    <td><span class="status-word ${escapeHtml(status)}">${escapeHtml(status)}</span></td>
                    <td class="col-num">${job.priority ?? 0}</td>
                    <td class="col-num">${job.attempts ?? 0}</td>
                    <td>${renderJobWorkerCell(job.worker_id)}</td>
                    <td class="mono" style="color:var(--muted);">${formatDate(job.created_at)}</td>
                </tr>
            `;
        })
        .join("");
}

/* =========================================================
   CREATE JOBS — SINGLE + BULK
========================================================= */

function initializeCreateMode() {
    const singleBtn = document.getElementById("modeSingleBtn");
    const bulkBtn = document.getElementById("modeBulkBtn");
    const singleFields = document.getElementById("singleFields");
    const bulkFields = document.getElementById("bulkFields");

    function setMode(mode) {
        createMode = mode;
        singleBtn.classList.toggle("active", mode === "single");
        bulkBtn.classList.toggle("active", mode === "bulk");
        singleFields.classList.toggle("hidden", mode !== "single");
        bulkFields.classList.toggle("hidden", mode !== "bulk");
    }

    if (singleBtn) singleBtn.addEventListener("click", () => setMode("single"));
    if (bulkBtn) bulkBtn.addEventListener("click", () => setMode("bulk"));
}

function initializeJobTypeSelect() {
    const select = document.getElementById("jobType");
    const command = document.getElementById("command");
    const nameInput = document.getElementById("jobName");

    if (!select || !command) return;

    select.addEventListener("change", () => {
        if (select.value === "custom") {
            command.focus();
            return;
        }
        command.value = select.value;

        const selectedOption = select.options[select.selectedIndex];
        const presetName = selectedOption.dataset.name;
        if (presetName && nameInput) {
            nameInput.value = presetName;
        }
    });
}

async function createJobs(event) {
    event.preventDefault();
    if (isCreating) return;

    isCreating = true;

    const createButton = document.getElementById("createButton");
    if (createButton) {
        createButton.disabled = true;
        createButton.textContent = "Creating…";
    }

    const progress = document.getElementById("creationProgress");
    if (progress) progress.classList.remove("hidden");

    try {
        if (createMode === "bulk") {
            await createJobsBulk();
        } else {
            await createJobsSingle();
        }
    } finally {
        isCreating = false;
        if (createButton) {
            createButton.disabled = false;
            createButton.textContent = "Create jobs";
        }
        await loadJobs();
        if (progress) {
            setTimeout(() => progress.classList.add("hidden"), 1200);
        }
    }
}

async function createJobsSingle() {
    const prefix = document.getElementById("jobName").value.trim();
    const count = Number(document.getElementById("jobCount").value);
    const command = document.getElementById("command").value.trim();
    const priority = Number(document.getElementById("priority").value);
    const maxRetries = Number(document.getElementById("maxRetries").value);

    if (!prefix) {
        showToast("Enter a job name prefix");
        return;
    }
    if (!command) {
        showToast("Enter a command");
        return;
    }
    if (!Number.isInteger(count) || count < 1 || count > 500) {
        showToast("Job count must be between 1 and 500");
        return;
    }
    if (!Number.isInteger(priority) || priority < 0 || priority > 1000) {
        showToast("Priority must be between 0 and 1000");
        return;
    }
    if (!Number.isInteger(maxRetries) || maxRetries < 0 || maxRetries > 20) {
        showToast("Max retries must be between 0 and 20");
        return;
    }

    let created = 0;
    let failed = 0;

    updateProgress(0, `Creating ${count} jobs…`);

    for (let i = 1; i <= count; i++) {
        try {
            await apiRequest("/jobs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    name: `${prefix}-${i}`,
                    command,
                    priority,
                    max_retries: maxRetries
                })
            });
            created++;
        } catch (error) {
            console.error(`Job ${i} failed`, error);
            failed++;
        }

        updateProgress(Math.round((i / count) * 100), `Created ${created} / ${count}`);
    }

    if (failed === 0) {
        showToast(`Successfully created ${created} jobs`);
    } else {
        showToast(`Created ${created}; ${failed} failed`);
    }
}

function parseBulkLine(line, index) {
    const trimmed = line.trim();
    if (!trimmed) return null;

    const parts = trimmed.split(",").map(p => p.trim());

    if (parts.length === 1) {
        return {
            name: `BULK-JOB-${index}`,
            command: parts[0],
            priority: 100,
            max_retries: 3
        };
    }

    const [name, command, priority, maxRetries] = parts;

    return {
        name: name || `BULK-JOB-${index}`,
        command: command || "echo worker job",
        priority: Number.isFinite(Number(priority)) ? Number(priority) : 100,
        max_retries: Number.isFinite(Number(maxRetries)) ? Number(maxRetries) : 3
    };
}

async function createJobsBulk() {
    const raw = document.getElementById("bulkInput").value;
    const lines = raw.split("\n");

    const jobs = lines
        .map((line, i) => parseBulkLine(line, i + 1))
        .filter(Boolean);

    if (!jobs.length) {
        showToast("Enter at least one job, one per line");
        return;
    }

    if (jobs.length > 500) {
        showToast("Maximum 500 jobs per batch");
        return;
    }

    let created = 0;
    let failed = 0;

    updateProgress(0, `Creating ${jobs.length} jobs…`);

    for (let i = 0; i < jobs.length; i++) {
        try {
            await apiRequest("/jobs", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(jobs[i])
            });
            created++;
        } catch (error) {
            console.error(`Bulk job ${i + 1} failed`, error);
            failed++;
        }

        updateProgress(Math.round(((i + 1) / jobs.length) * 100), `Created ${created} / ${jobs.length}`);
    }

    if (failed === 0) {
        showToast(`Successfully created ${created} jobs`);
    } else {
        showToast(`Created ${created}; ${failed} failed`);
    }
}

function updateProgress(percent, text) {
    const fill = document.getElementById("progressFill");
    const progressPercent = document.getElementById("progressPercent");
    const progressText = document.getElementById("progressText");

    if (fill) fill.style.width = `${percent}%`;
    if (progressPercent) progressPercent.textContent = `${percent}%`;
    if (progressText) progressText.textContent = text;
}

/* =========================================================
   CLEAR ALL JOBS
========================================================= */

async function clearAllJobs() {
    const confirmed = confirm(
        "Delete ALL jobs?\n\nThis cannot be undone."
    );
    if (!confirmed) return;

    try {
        const result = await apiRequest("/jobs/all", { method: "DELETE" });
        showToast(result.message || "Jobs cleared");
        await loadJobs();
    } catch (error) {
        console.error("Failed to clear jobs:", error);
        showToast(`Failed: ${error.message}`);
    }
}

/* =========================================================
   HELPERS
========================================================= */

function setText(id, value) {
    const el = document.getElementById(id);
    if (el) el.textContent = value;
}

function formatDate(value) {
    if (!value) return "—";
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) return value;
    return date.toLocaleString();
}

function escapeHtml(value) {
    if (value === null || value === undefined) return "";
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function showToast(message) {
    const toast = document.getElementById("toast");
    if (!toast) return;
    toast.textContent = message;
    toast.classList.add("show");
    setTimeout(() => toast.classList.remove("show"), 3000);
}

function setTextValue(id, value) {
    const el = document.getElementById(id);
    if (el) el.value = value;
}

/* =========================================================
   EVENTS
========================================================= */

document.querySelectorAll("[data-count]").forEach(button => {
    button.addEventListener("click", () => {
        const input = document.getElementById("jobCount");
        if (input) input.value = button.dataset.count;
    });
});

const jobForm = document.getElementById("jobForm");
if (jobForm) jobForm.addEventListener("submit", createJobs);

const refreshButton = document.getElementById("refreshButton");
if (refreshButton) {
    refreshButton.addEventListener("click", async () => {
        await Promise.all([loadJobs(), loadWorkers()]);
    });
}

const searchInput = document.getElementById("searchInput");
if (searchInput) searchInput.addEventListener("input", renderJobs);

const statusFilter = document.getElementById("statusFilter");
if (statusFilter) statusFilter.addEventListener("change", renderJobs);

const clearButton = document.getElementById("clearButton");
if (clearButton) {
    clearButton.addEventListener("click", () => {
        setTextValue("jobName", "DISTRIBUTED-JOB");
        setTextValue("jobCount", "1");
        setTextValue("command", "echo worker job");
        setTextValue("priority", "100");
        setTextValue("maxRetries", "3");
        setTextValue("bulkInput", "");
    });
}

const clearAllJobsButton = document.getElementById("clearAllJobsButton");
if (clearAllJobsButton) clearAllJobsButton.addEventListener("click", clearAllJobs);

/* =========================================================
   INIT
========================================================= */

function init() {
    initializeCapacityControls();
    initializeFleetSelection();
    initializeCreateMode();
    initializeJobTypeSelect();

    loadJobs();
    loadWorkers();

    setInterval(loadJobs, 2000);
    setInterval(loadWorkers, 2000);
}

if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
} else {
    init();
}