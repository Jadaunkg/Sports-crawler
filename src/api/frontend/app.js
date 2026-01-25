// Sports Crawler Dashboard - JavaScript

const API_BASE = '';  // Same origin

// State
let currentPage = 0;
const pageSize = 50;
let pollInterval = null;

// ==================== MOBILE MENU ====================

function toggleMobileMenu() {
    const sidebar = document.getElementById('sidebar');
    sidebar.classList.toggle('active');
}

// Close mobile menu when clicking outside
document.addEventListener('click', (e) => {
    const sidebar = document.getElementById('sidebar');
    const toggle = document.querySelector('.mobile-menu-toggle');
    
    if (sidebar && sidebar.classList.contains('active') && 
        !sidebar.contains(e.target) && !toggle.contains(e.target)) {
        sidebar.classList.remove('active');
    }
});

// ==================== NAVIGATION ====================

function showView(viewName) {
    // Hide all views
    document.querySelectorAll('.view').forEach(v => v.classList.remove('active'));
    document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));

    // Show selected view
    document.getElementById(`view-${viewName}`).classList.add('active');
    document.querySelector(`[data-view="${viewName}"]`).classList.add('active');
    
    // Close mobile menu
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.remove('active');

    // Load data for the view
    switch (viewName) {
        case 'dashboard':
            loadDashboardData();
            break;
        case 'sites':
            loadSites();
            break;
        case 'articles':
            loadArticles();
            break;
        case 'crawl':
            loadCrawlStatus();
            populateSiteSelector();
            break;
    }
}

// Initialize navigation
document.querySelectorAll('.nav-item').forEach(item => {
    item.addEventListener('click', (e) => {
        e.preventDefault();
        const view = item.dataset.view;
        showView(view);
    });
});

// ==================== DASHBOARD ====================

async function loadDashboardData() {
    try {
        // Load sites count
        const sites = await fetch(`${API_BASE}/api/sites`).then(r => r.json());
        document.getElementById('stat-sites').textContent = sites.filter(s => s.is_active).length;

        // Load articles count
        const articlesCount = await fetch(`${API_BASE}/api/articles/count`).then(r => r.json());
        document.getElementById('stat-articles').textContent = articlesCount.count || 0;

        // Load crawl status
        const status = await fetch(`${API_BASE}/api/crawl/status`).then(r => r.json());
        document.getElementById('stat-status').textContent = status.is_running ? 'Running' : 'Ready';

        // Load recent articles
        const articles = await fetch(`${API_BASE}/api/articles?limit=5`).then(r => r.json());
        renderRecentArticles(articles);

    } catch (err) {
        console.error('Error loading dashboard:', err);
    }
}

function renderRecentArticles(articles) {
    const container = document.getElementById('recent-articles-list');

    if (!articles.length) {
        container.innerHTML = '<p class="text-muted text-center">No articles yet. Run a crawl to get started!</p>';
        return;
    }

    container.innerHTML = articles.map(a => `
        <div class="article-card">
            <a href="${a.url}" target="_blank" class="article-card-title">${escapeHtml(a.title)}</a>
            <div class="article-card-meta">
                <span><i class="fas fa-globe"></i> ${escapeHtml(a.source_site)}</span>
                <span class="category-badge"><i class="fas fa-tag"></i> ${a.sport_category || 'sports'}</span>
            </div>
        </div>
    `).join('');
}

// ==================== SITES ====================

async function loadSites() {
    try {
        const sites = await fetch(`${API_BASE}/api/sites`).then(r => r.json());
        renderSites(sites);
    } catch (err) {
        console.error('Error loading sites:', err);
    }
}

function renderSites(sites) {
    const container = document.getElementById('sites-grid');

    if (!sites.length) {
        container.innerHTML = `
            <div class="site-card">
                <p class="text-center">No sites configured. Add a site to get started!</p>
            </div>
        `;
        return;
    }

    container.innerHTML = sites.map(s => `
        <div class="site-card ${s.is_active ? '' : 'inactive'}">
            <div class="site-header">
                <span class="site-name">${escapeHtml(s.name)}</span>
                <span class="site-badge ${s.is_active ? 'active' : 'inactive'}">
                    ${s.is_active ? 'Active' : 'Inactive'}
                </span>
            </div>
            <div class="site-domain">${escapeHtml(s.domain)}</div>
            <div class="site-meta">
                <span><i class="fas fa-clock"></i> Every ${s.crawl_interval_minutes} min</span>
                <span><i class="fas fa-folder"></i> ${s.site_type || 'specific'}</span>
            </div>
            <div class="site-actions">
                <button class="btn btn-secondary" onclick="toggleSite('${s.id}')">
                    <i class="fas fa-${s.is_active ? 'pause' : 'play'}"></i>
                    ${s.is_active ? 'Pause' : 'Enable'}
                </button>
                <button class="btn btn-danger" onclick="deleteSite('${s.id}')">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        </div>
    `).join('');
}

async function toggleSite(siteId) {
    try {
        await fetch(`${API_BASE}/api/sites/${siteId}/toggle`, { method: 'PATCH' });
        loadSites();
    } catch (err) {
        console.error('Error toggling site:', err);
    }
}

async function deleteSite(siteId) {
    if (!confirm('Are you sure you want to delete this site?')) return;

    try {
        await fetch(`${API_BASE}/api/sites/${siteId}`, { method: 'DELETE' });
        loadSites();
    } catch (err) {
        console.error('Error deleting site:', err);
    }
}

// ==================== ADD SITE MODAL ====================

function openAddSiteModal() {
    document.getElementById('add-site-modal').classList.add('active');
}

function closeAddSiteModal() {
    document.getElementById('add-site-modal').classList.remove('active');
    document.getElementById('add-site-form').reset();
}

function toggleSportFocus() {
    const siteType = document.getElementById('site-type').value;
    const focusGroup = document.getElementById('sport-focus-group');
    focusGroup.style.display = siteType === 'specific' ? 'block' : 'none';
}

async function addSite(event) {
    event.preventDefault();

    const siteData = {
        name: document.getElementById('site-name').value,
        domain: document.getElementById('site-domain').value,
        sitemap_url: document.getElementById('site-sitemap').value,
        site_type: document.getElementById('site-type').value,
        sport_focus: document.getElementById('sport-focus').value,
        crawl_interval_minutes: parseInt(document.getElementById('crawl-interval').value)
    };

    try {
        const response = await fetch(`${API_BASE}/api/sites`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(siteData)
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Error adding site');
            return;
        }

        closeAddSiteModal();
        loadSites();
        showView('sites');

    } catch (err) {
        console.error('Error adding site:', err);
        alert('Error adding site');
    }
}

// ==================== ARTICLES ====================

async function loadArticles() {
    const source = document.getElementById('filter-source').value;
    const category = document.getElementById('filter-category').value;

    let url = `${API_BASE}/api/articles?limit=${pageSize}&offset=${currentPage * pageSize}`;
    if (source) url += `&source=${source}`;
    if (category) url += `&category=${category}`;

    try {
        const articles = await fetch(url).then(r => r.json());
        renderArticlesTable(articles);

        // Update source filter
        await populateSourceFilter();

    } catch (err) {
        console.error('Error loading articles:', err);
    }
}

async function populateSourceFilter() {
    try {
        const sites = await fetch(`${API_BASE}/api/sites`).then(r => r.json());
        const select = document.getElementById('filter-source');

        // Keep first option
        const firstOption = select.options[0];
        select.innerHTML = '';
        select.appendChild(firstOption);

        sites.forEach(s => {
            const opt = document.createElement('option');
            opt.value = s.name;
            opt.textContent = s.name;
            select.appendChild(opt);
        });
    } catch (err) {
        console.error('Error populating source filter:', err);
    }
}

function renderArticlesTable(articles) {
    const tbody = document.getElementById('articles-tbody');

    if (!articles.length) {
        tbody.innerHTML = `
            <tr>
                <td colspan="5" style="text-align: center; padding: 48px 32px;">
                    <i class="fas fa-inbox" style="font-size: 48px; color: var(--text-muted); margin-bottom: 16px; display: block;"></i>
                    <p style="color: var(--text-muted); font-size: 16px;">No articles found. Run a crawl to fetch articles!</p>
                </td>
            </tr>
        `;
        return;
    }

    tbody.innerHTML = articles.map(a => `
        <tr>
            <td class="article-title">
                <a href="${a.url}" target="_blank">${escapeHtml(a.title)}</a>
            </td>
            <td>${escapeHtml(a.source_site)}</td>
            <td><span class="category-badge">${a.sport_category || 'sports'}</span></td>
            <td>${formatDate(a.publish_date || a.crawl_time)}</td>
            <td>
                <a href="${a.url}" target="_blank" class="btn btn-secondary" style="padding: 6px 12px;">
                    <i class="fas fa-external-link-alt"></i>
                </a>
            </td>
        </tr>
    `).join('');

    document.getElementById('page-info').textContent = `Page ${currentPage + 1}`;
}

function prevPage() {
    if (currentPage > 0) {
        currentPage--;
        loadArticles();
    }
}

function nextPage() {
    currentPage++;
    loadArticles();
}

// ==================== CRAWL ====================

async function populateSiteSelector() {
    try {
        const sites = await fetch(`${API_BASE}/api/sites`).then(r => r.json());
        const select = document.getElementById('crawl-sites');

        // Keep first option
        const firstOption = select.options[0];
        select.innerHTML = '';
        select.appendChild(firstOption);

        sites.filter(s => s.is_active).forEach(s => {
            const opt = document.createElement('option');
            opt.value = s.id;
            opt.textContent = s.name;
            select.appendChild(opt);
        });
    } catch (err) {
        console.error('Error populating site selector:', err);
    }
}

async function startCrawl() {
    const days = parseInt(document.getElementById('crawl-days').value);
    const siteId = document.getElementById('crawl-sites').value;

    const requestBody = {
        days: days,
        site_ids: siteId ? [siteId] : null
    };

    try {
        const response = await fetch(`${API_BASE}/api/crawl/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(requestBody)
        });

        if (!response.ok) {
            const err = await response.json();
            alert(err.detail || 'Error starting crawl');
            return;
        }

        // Start polling for status
        startStatusPolling();

        document.getElementById('btn-start-crawl').disabled = true;
        document.getElementById('btn-stop-crawl').disabled = false;

    } catch (err) {
        console.error('Error starting crawl:', err);
        alert('Error starting crawl');
    }
}

async function stopCrawl() {
    try {
        await fetch(`${API_BASE}/api/crawl/stop`, { method: 'POST' });
        stopStatusPolling();

        document.getElementById('btn-start-crawl').disabled = false;
        document.getElementById('btn-stop-crawl').disabled = true;

    } catch (err) {
        console.error('Error stopping crawl:', err);
    }
}

async function loadCrawlStatus() {
    try {
        const status = await fetch(`${API_BASE}/api/crawl/status`).then(r => r.json());
        updateCrawlStatusUI(status);
    } catch (err) {
        console.error('Error loading crawl status:', err);
    }
}

function updateCrawlStatusUI(status) {
    const indicator = document.getElementById('status-indicator');
    const statusText = document.getElementById('status-text');
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const savedText = document.getElementById('saved-text');
    const log = document.getElementById('crawl-log');

    // Update indicator
    indicator.className = 'status-indicator ' + (status.is_running ? 'running' : 'stopped');

    // Update status text
    if (status.is_running) {
        statusText.textContent = `Crawling: ${status.current_site || 'Initializing...'}`;
    } else {
        statusText.textContent = status.last_run ? `Last run: ${formatDate(status.last_run)}` : 'Ready to crawl';
    }

    // Update progress
    const progress = status.total > 0 ? (status.progress / status.total) * 100 : 0;
    progressBar.style.width = `${progress}%`;
    progressText.textContent = `${status.progress} / ${status.total} articles`;
    savedText.textContent = `${status.articles_saved} saved`;

    // Update buttons
    document.getElementById('btn-start-crawl').disabled = status.is_running;
    document.getElementById('btn-stop-crawl').disabled = !status.is_running;

    // Update log
    if (status.errors && status.errors.length > 0) {
        log.innerHTML = status.errors.slice(-10).map(e =>
            `<p class="log-entry error"><i class="fas fa-exclamation-triangle"></i> ${escapeHtml(e)}</p>`
        ).join('');
    } else if (status.is_running) {
        log.innerHTML = '<p class="log-entry"><i class="fas fa-spinner fa-spin"></i> Crawl in progress...</p>';
    }
}

function startStatusPolling() {
    if (pollInterval) return;
    pollInterval = setInterval(loadCrawlStatus, 2000);
}

function stopStatusPolling() {
    if (pollInterval) {
        clearInterval(pollInterval);
        pollInterval = null;
    }
}

// ==================== UTILITIES ====================

function escapeHtml(text) {
    if (!text) return '';
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function formatDate(dateStr) {
    if (!dateStr) return '-';
    try {
        const date = new Date(dateStr);
        return date.toLocaleDateString('en-US', {
            month: 'short',
            day: 'numeric',
            hour: '2-digit',
            minute: '2-digit'
        });
    } catch {
        return dateStr;
    }
}

// ==================== INIT ====================

document.addEventListener('DOMContentLoaded', () => {
    showView('dashboard');
});
