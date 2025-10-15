document.addEventListener('DOMContentLoaded', function () {
    const mainContent = document.getElementById('main-content');
    const navLinks = document.querySelectorAll('.nav-link');
    let logRefreshInterval = null;
    let taskRefreshInterval = null;

    // --- Templates for each section ---
    const templates = {
        tasks: () => `
            <section id="tasks-section" class="content-section">
                <div class="section-header">
                    <h2>任务管理</h2>
                    <button id="add-task-btn" class="control-button primary-btn">➕ 创建新任务</button>
                </div>
                <div id="tasks-table-container">
                    <p>正在加载任务列表...</p>
                </div>
            </section>`,
        results: () => `
            <section id="results-section" class="content-section">
                <div class="section-header">
                    <h2>结果查看</h2>
                </div>
                <div class="results-filter-bar">
                    <select id="result-file-selector"><option>加载中...</option></select>
                    <label>
                        <input type="checkbox" id="recommended-only-checkbox">
                        仅看AI推荐
                    </label>
                    <select id="sort-by-selector">
                        <option value="crawl_time">按爬取时间</option>
                        <option value="publish_time">按发布时间</option>
                        <option value="price">按价格</option>
                    </select>
                    <select id="sort-order-selector">
                        <option value="desc">降序</option>
                        <option value="asc">升序</option>
                    </select>
                    <button id="refresh-results-btn" class="control-button">🔄 刷新</button>
                </div>
                <div id="results-grid-container">
                    <p>请先选择一个结果文件。</p>
                </div>
            </section>`,
        logs: () => `
            <section id="logs-section" class="content-section">
                <div class="section-header">
                    <h2>运行日志</h2>
                    <div class="log-controls">
                        <label>
                            <input type="checkbox" id="auto-refresh-logs-checkbox">
                            自动刷新
                        </label>
                        <button id="refresh-logs-btn" class="control-button">🔄 刷新</button>
                        <button id="clear-logs-btn" class="control-button danger-btn">🗑️ 清空日志</button>
                    </div>
                </div>
                <pre id="log-content-container">正在加载日志...</pre>
            </section>`,
        settings: () => `
            <section id="settings-section" class="content-section">
                <h2>系统设置</h2>
                <div class="settings-card">
                    <h3>系统状态检查</h3>
                    <div id="system-status-container"><p>正在加载状态...</p></div>
                </div>
                <div class="settings-card">
                    <h3>Prompt 管理</h3>
                    <div class="prompt-manager">
                        <div class="prompt-list-container">
                            <label for="prompt-selector">选择要编辑的 Prompt:</label>
                            <select id="prompt-selector"><option>加载中...</option></select>
                        </div>
                        <div class="prompt-editor-container">
                            <textarea id="prompt-editor" spellcheck="false" disabled placeholder="请先从上方选择一个 Prompt 文件进行编辑..."></textarea>
                            <button id="save-prompt-btn" class="control-button primary-btn" disabled>保存更改</button>
                        </div>
                    </div>
                </div>
            </section>`
    };

    // --- API Functions ---
    async function fetchPrompts() {
        try {
            const response = await fetch('/api/prompts');
            if (!response.ok) throw new Error('无法获取Prompt列表');
            return await response.json();
        } catch (error) {
            console.error(error);
            return [];
        }
    }

    async function fetchPromptContent(filename) {
        try {
            const response = await fetch(`/api/prompts/${filename}`);
            if (!response.ok) throw new Error(`无法获取Prompt文件 ${filename} 的内容`);
            return await response.json();
        } catch (error) {
            console.error(error);
            return null;
        }
    }

    async function updatePrompt(filename, content) {
        try {
            const response = await fetch(`/api/prompts/${filename}`, {
                method: 'PUT',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({content: content}),
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '更新Prompt失败');
            }
            return await response.json();
        } catch (error) {
            console.error(`无法更新Prompt ${filename}:`, error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function createTaskWithAI(data) {
        try {
            const response = await fetch(`/api/tasks/generate`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data),
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '通过AI创建任务失败');
            }
            console.log(`AI任务创建成功!`);
            return await response.json();
        } catch (error) {
            console.error(`无法通过AI创建任务:`, error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function startSingleTask(taskId) {
        try {
            const response = await fetch(`/api/tasks/start/${taskId}`, {
                method: 'POST',
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '启动任务失败');
            }
            return await response.json();
        } catch (error) {
            console.error(`无法启动任务 ${taskId}:`, error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function stopSingleTask(taskId) {
        try {
            const response = await fetch(`/api/tasks/stop/${taskId}`, {
                method: 'POST',
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '停止任务失败');
            }
            return await response.json();
        } catch (error) {
            console.error(`无法停止任务 ${taskId}:`, error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function deleteTask(taskId) {
        try {
            const response = await fetch(`/api/tasks/${taskId}`, {
                method: 'DELETE',
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '删除任务失败');
            }
            console.log(`任务 ${taskId} 删除成功!`);
            return await response.json();
        } catch (error) {
            console.error(`无法删除任务 ${taskId}:`, error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function updateTask(taskId, data) {
        try {
            const response = await fetch(`/api/tasks/${taskId}`, {
                method: 'PATCH',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify(data),
            });
            if (!response.ok) {
                const errorData = await response.json();
                throw new Error(errorData.detail || '更新任务失败');
            }
            console.log(`任务 ${taskId} 更新成功!`);
            return await response.json();
        } catch (error) {
            console.error(`无法更新任务 ${taskId}:`, error);
            // TODO: Use a more elegant notification system
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function fetchTasks() {
        try {
            const response = await fetch('/api/tasks');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error("无法获取任务列表:", error);
            return null;
        }
    }

    async function fetchResultFiles() {
        try {
            const response = await fetch('/api/results/files');
            if (!response.ok) throw new Error('无法获取结果文件列表');
            return await response.json();
        } catch (error) {
            console.error(error);
            return null;
        }
    }

    async function fetchResultContent(filename, recommendedOnly, sortBy, sortOrder) {
        try {
            const params = new URLSearchParams({
                page: 1,
                limit: 100, // Fetch a decent number of items
                recommended_only: recommendedOnly,
                sort_by: sortBy,
                sort_order: sortOrder
            });
            const response = await fetch(`/api/results/${filename}?${params}`);
            if (!response.ok) throw new Error(`无法获取文件 ${filename} 的内容`);
            return await response.json();
        } catch (error) {
            console.error(error);
            return null;
        }
    }

    async function fetchSystemStatus() {
        try {
            const response = await fetch('/api/settings/status');
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error("无法获取系统状态:", error);
            return null;
        }
    }

    async function clearLogs() {
        try {
            const response = await fetch('/api/logs', {method: 'DELETE'});
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || '清空日志失败');
            }
            return await response.json();
        } catch (error) {
            console.error("无法清空日志:", error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function deleteLoginState() {
        try {
            const response = await fetch('/api/login-state', {method: 'DELETE'});
            if (!response.ok) {
                const err = await response.json();
                throw new Error(err.detail || '删除登录凭证失败');
            }
            return await response.json();
        } catch (error) {
            console.error("无法删除登录凭证:", error);
            alert(`错误: ${error.message}`);
            return null;
        }
    }

    async function fetchLogs(fromPos = 0) {
        try {
            const response = await fetch(`/api/logs?from_pos=${fromPos}`);
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return await response.json();
        } catch (error) {
            console.error("无法获取日志:", error);
            return {new_content: `\n加载日志失败: ${error.message}`, new_pos: fromPos};
        }
    }

    // --- Render Functions ---
    function renderLoginStatusWidget(status) {
        const container = document.getElementById('login-status-widget-container');
        if (!container) return;

        const loginState = status.login_state_file;
        let content = '';

        if (loginState && loginState.exists) {
            content = `
                <div class="login-status-widget">
                    <span class="status-text status-ok">✓ 已登录</span>
                    <div class="dropdown-menu">
                        <a href="#" class="dropdown-item" id="update-login-state-btn-widget">手动更新</a>
                        <a href="#" class="dropdown-item delete" id="delete-login-state-btn-widget">删除凭证</a>
                    </div>
                </div>
            `;
        } else {
            content = `
                <div class="login-status-widget">
                    <span class="status-text status-error" id="update-login-state-btn-widget">! 闲鱼未登录 (点击设置)</span>
                </div>
            `;
        }
        container.innerHTML = content;
    }

    async function refreshLoginStatusWidget() {
        const status = await fetchSystemStatus();
        if (status) {
            renderLoginStatusWidget(status);
        }
    }

    function renderSystemStatus(status) {
        if (!status) return '<p>无法加载系统状态。</p>';

        const renderStatusTag = (isOk) => isOk
            ? `<span class="tag status-ok">正常</span>`
            : `<span class="tag status-error">异常</span>`;

        const env = status.env_file || {};

        return `
            <ul class="status-list">
                <li class="status-item">
                    <span class="label">环境变量文件 (.env)</span>
                    <span class="value">${renderStatusTag(env.exists)}</span>
                </li>
                <li class="status-item">
                    <span class="label">OpenAI API Key</span>
                    <span class="value">${renderStatusTag(env.openai_api_key_set)}</span>
                </li>
                <li class="status-item">
                    <span class="label">OpenAI Base URL</span>
                    <span class="value">${renderStatusTag(env.openai_base_url_set)}</span>
                </li>
                <li class="status-item">
                    <span class="label">OpenAI Model Name</span>
                    <span class="value">${renderStatusTag(env.openai_model_name_set)}</span>
                </li>
                <li class="status-item">
                    <span class="label">Ntfy Topic URL</span>
                    <span class="value">${renderStatusTag(env.ntfy_topic_url_set)}</span>
                </li>
            </ul>
        `;
    }

    function renderResultsGrid(data) {
        if (!data || !data.items || data.items.length === 0) {
            return '<p>没有找到符合条件的商品记录。</p>';
        }

        const cards = data.items.map(item => {
            const info = item.商品信息 || {};
            const seller = item.卖家信息 || {};
            const ai = item.ai_analysis || {};

            const isRecommended = ai.is_recommended === true;
            const recommendationClass = isRecommended ? 'recommended' : 'not-recommended';
            const recommendationText = isRecommended ? '推荐' : (ai.is_recommended === false ? '不推荐' : '待定');

            const imageUrl = (info.商品图片列表 && info.商品图片列表[0]) ? info.商品图片列表[0] : 'data:image/gif;base64,R0lGODlhAQABAAD/ACwAAAAAAQABAAACADs=';
            const crawlTime = item.爬取时间 ? new Date(item.爬取时间).toLocaleString('sv-SE').slice(0, 16) : '未知';
            const publishTime = info.发布时间 || '未知';

            return `
            <div class="result-card" data-item='${JSON.stringify(item)}'>
                <div class="card-image">
                    <a href="${info.商品链接 || '#'}" target="_blank"><img src="${imageUrl}" alt="${info.商品标题 || '商品图片'}" loading="lazy"></a>
                </div>
                <div class="card-content">
                    <h3 class="card-title"><a href="${info.商品链接 || '#'}" target="_blank" title="${info.商品标题 || ''}">${info.商品标题 || '无标题'}</a></h3>
                    <p class="card-price">${info.当前售价 || '价格未知'}</p>
                    <div class="card-ai-summary ${recommendationClass}">
                        <strong>AI建议: ${recommendationText}</strong>
                        <p title="${ai.reason || ''}">原因: ${ai.reason || '无分析'}</p>
                    </div>
                    <div class="card-footer">
                        <div>
                            <span class="seller-info" title="${info.卖家昵称 || seller.卖家昵称 || '未知'}">卖家: ${info.卖家昵称 || seller.卖家昵称 || '未知'}</span>
                            <div class="time-info">
                                <p>发布于: ${publishTime}</p>
                                <p>抓取于: ${crawlTime}</p>
                            </div>
                        </div>
                        <a href="${info.商品链接 || '#'}" target="_blank" class="action-btn">查看详情</a>
                    </div>
                </div>
            </div>
            `;
        }).join('');

        return `<div id="results-grid">${cards}</div>`;
    }

    function renderTasksTable(tasks) {
        if (!tasks || tasks.length === 0) {
            return '<p>没有找到任何任务。请点击右上角“创建新任务”来添加一个。</p>';
        }

        const tableHeader = `
            <thead>
                <tr>
                    <th>启用</th>
                    <th>任务名称</th>
                    <th>运行状态</th>
                    <th>关键词</th>
                    <th>价格范围</th>
                    <th>筛选条件</th>
                    <th>最大页数</th>
                    <th>AI 标准</th>
                    <th>定时规则</th>
                    <th>操作</th>
                </tr>
            </thead>`;

        const tableBody = tasks.map(task => {
            const isRunning = task.is_running === true;
            const statusBadge = isRunning
                ? `<span class="status-badge status-running">运行中</span>`
                : `<span class="status-badge status-stopped">已停止</span>`;

            const actionButton = isRunning
                ? `<button class="action-btn stop-task-btn" data-task-id="${task.id}">停止</button>`
                : `<button class="action-btn run-task-btn" data-task-id="${task.id}" ${!task.enabled ? 'disabled title="任务已禁用"' : ''}>运行</button>`;

            return `
            <tr data-task-id="${task.id}" data-task='${JSON.stringify(task)}'>
                <td>
                    <label class="switch">
                        <input type="checkbox" ${task.enabled ? 'checked' : ''}>
                        <span class="slider round"></span>
                    </label>
                </td>
                <td>${task.task_name}</td>
                <td>${statusBadge}</td>
                <td><span class="tag">${task.keyword}</span></td>
                <td>${task.min_price || '不限'} - ${task.max_price || '不限'}</td>
                <td>${task.personal_only ? '<span class="tag personal">个人闲置</span>' : ''}</td>
                <td>${task.max_pages || 3}</td>
                <td>${(task.ai_prompt_criteria_file || 'N/A').replace('prompts/', '')}</td>
                <td>${task.cron || '未设置'}</td>
                <td>
                    ${actionButton}
                    <button class="action-btn edit-btn">编辑</button>
                    <button class="action-btn delete-btn">删除</button>
                </td>
            </tr>`
        }).join('');

        return `<table class="tasks-table">${tableHeader}<tbody>${tableBody}</tbody></table>`;
    }


    async function navigateTo(hash) {
        if (logRefreshInterval) {
            clearInterval(logRefreshInterval);
            logRefreshInterval = null;
        }
        if (taskRefreshInterval) {
            clearInterval(taskRefreshInterval);
            taskRefreshInterval = null;
        }
        const sectionId = hash.substring(1) || 'tasks';

        // Update nav links active state
        navLinks.forEach(link => {
            link.classList.toggle('active', link.getAttribute('href') === `#${sectionId}`);
        });

        // Update main content
        if (templates[sectionId]) {
            mainContent.innerHTML = templates[sectionId]();
            // Make the new content visible
            const newSection = mainContent.querySelector('.content-section');
            if (newSection) {
                requestAnimationFrame(() => {
                    newSection.classList.add('active');
                });
            }

            // --- Load data for the current section ---
            if (sectionId === 'tasks') {
                const container = document.getElementById('tasks-table-container');
                const refreshTasks = async () => {
                    const tasks = await fetchTasks();
                    // Avoid re-rendering if in edit mode to not lose user input
                    if (container && !container.querySelector('tr.editing')) {
                        container.innerHTML = renderTasksTable(tasks);
                    }
                };
                await refreshTasks();
                taskRefreshInterval = setInterval(refreshTasks, 5000);
            } else if (sectionId === 'results') {
                await initializeResultsView();
            } else if (sectionId === 'logs') {
                await initializeLogsView();
            } else if (sectionId === 'settings') {
                await initializeSettingsView();
            }

        } else {
            mainContent.innerHTML = '<section class="content-section active"><h2>页面未找到</h2></section>';
        }
    }

    async function initializeLogsView() {
        const logContainer = document.getElementById('log-content-container');
        const refreshBtn = document.getElementById('refresh-logs-btn');
        const autoRefreshCheckbox = document.getElementById('auto-refresh-logs-checkbox');
        const clearBtn = document.getElementById('clear-logs-btn');
        let currentLogSize = 0;

        const updateLogs = async (isFullRefresh = false) => {
            // For incremental updates, check if user is at the bottom BEFORE adding new content.
            const shouldAutoScroll = isFullRefresh || (logContainer.scrollHeight - logContainer.clientHeight <= logContainer.scrollTop + 5);

            if (isFullRefresh) {
                currentLogSize = 0;
                logContainer.textContent = '正在加载...';
            }

            const logData = await fetchLogs(currentLogSize);

            if (isFullRefresh) {
                // If the log is empty, show a message instead of a blank screen.
                logContainer.textContent = logData.new_content || '日志为空，等待内容...';
            } else if (logData.new_content) {
                // If it was showing the empty message, replace it.
                if (logContainer.textContent === '日志为空，等待内容...') {
                    logContainer.textContent = logData.new_content;
                } else {
                    logContainer.textContent += logData.new_content;
                }
            }
            currentLogSize = logData.new_pos;

            // Scroll to bottom if it was a full refresh or if the user was already at the bottom.
            if (shouldAutoScroll) {
                logContainer.scrollTop = logContainer.scrollHeight;
            }
        };

        refreshBtn.addEventListener('click', () => updateLogs(true));

        clearBtn.addEventListener('click', async () => {
            if (confirm('你确定要清空所有运行日志吗？此操作不可恢复。')) {
                const result = await clearLogs();
                if (result) {
                    await updateLogs(true);
                    alert('日志已清空。');
                }
            }
        });

        autoRefreshCheckbox.addEventListener('change', () => {
            if (autoRefreshCheckbox.checked) {
                if (logRefreshInterval) clearInterval(logRefreshInterval);
                logRefreshInterval = setInterval(() => updateLogs(false), 1000);
            } else {
                if (logRefreshInterval) {
                    clearInterval(logRefreshInterval);
                    logRefreshInterval = null;
                }
            }
        });

        await updateLogs(true);
    }

    async function fetchAndRenderResults() {
        const selector = document.getElementById('result-file-selector');
        const checkbox = document.getElementById('recommended-only-checkbox');
        const sortBySelector = document.getElementById('sort-by-selector');
        const sortOrderSelector = document.getElementById('sort-order-selector');
        const container = document.getElementById('results-grid-container');

        if (!selector || !checkbox || !container || !sortBySelector || !sortOrderSelector) return;

        const selectedFile = selector.value;
        const recommendedOnly = checkbox.checked;
        const sortBy = sortBySelector.value;
        const sortOrder = sortOrderSelector.value;

        if (!selectedFile) {
            container.innerHTML = '<p>请先选择一个结果文件。</p>';
            return;
        }

        localStorage.setItem('lastSelectedResultFile', selectedFile);

        container.innerHTML = '<p>正在加载结果...</p>';
        const data = await fetchResultContent(selectedFile, recommendedOnly, sortBy, sortOrder);
        container.innerHTML = renderResultsGrid(data);
    }

    async function initializeResultsView() {
        const selector = document.getElementById('result-file-selector');
        const checkbox = document.getElementById('recommended-only-checkbox');
        const refreshBtn = document.getElementById('refresh-results-btn');
        const sortBySelector = document.getElementById('sort-by-selector');
        const sortOrderSelector = document.getElementById('sort-order-selector');

        const fileData = await fetchResultFiles();
        if (fileData && fileData.files && fileData.files.length > 0) {
            const lastSelectedFile = localStorage.getItem('lastSelectedResultFile');
            // Determine the file to select. Default to the first file if nothing is stored or if the stored file no longer exists.
            let fileToSelect = fileData.files[0];
            if (lastSelectedFile && fileData.files.includes(lastSelectedFile)) {
                fileToSelect = lastSelectedFile;
            }

            selector.innerHTML = fileData.files.map(f =>
                `<option value="${f}" ${f === fileToSelect ? 'selected' : ''}>${f}</option>`
            ).join('');

            // The selector's value is now correctly set by the 'selected' attribute.
            // We can proceed with adding listeners and the initial fetch.

            selector.addEventListener('change', fetchAndRenderResults);
            checkbox.addEventListener('change', fetchAndRenderResults);
            sortBySelector.addEventListener('change', fetchAndRenderResults);
            sortOrderSelector.addEventListener('change', fetchAndRenderResults);
            refreshBtn.addEventListener('click', fetchAndRenderResults);
            // Initial load
            await fetchAndRenderResults();
        } else {
            selector.innerHTML = '<option value="">没有可用的结果文件</option>';
            document.getElementById('results-grid-container').innerHTML = '<p>没有找到任何结果文件。请先运行监控任务。</p>';
        }
    }

    async function initializeSettingsView() {
        // 1. Render System Status
        const statusContainer = document.getElementById('system-status-container');
        const status = await fetchSystemStatus();
        statusContainer.innerHTML = renderSystemStatus(status);

        // 2. Setup Prompt Editor
        const promptSelector = document.getElementById('prompt-selector');
        const promptEditor = document.getElementById('prompt-editor');
        const savePromptBtn = document.getElementById('save-prompt-btn');

        const prompts = await fetchPrompts();
        if (prompts && prompts.length > 0) {
            promptSelector.innerHTML = '<option value="">-- 请选择 --</option>' + prompts.map(p => `<option value="${p}">${p}</option>`).join('');
        } else {
            promptSelector.innerHTML = '<option value="">没有找到Prompt文件</option>';
        }

        promptSelector.addEventListener('change', async () => {
            const selectedFile = promptSelector.value;
            if (selectedFile) {
                promptEditor.value = "正在加载...";
                promptEditor.disabled = true;
                savePromptBtn.disabled = true;
                const data = await fetchPromptContent(selectedFile);
                if (data) {
                    promptEditor.value = data.content;
                    promptEditor.disabled = false;
                    savePromptBtn.disabled = false;
                } else {
                    promptEditor.value = `加载文件 ${selectedFile} 失败。`;
                }
            } else {
                promptEditor.value = "请先从上方选择一个 Prompt 文件进行编辑...";
                promptEditor.disabled = true;
                savePromptBtn.disabled = true;
            }
        });

        savePromptBtn.addEventListener('click', async () => {
            const selectedFile = promptSelector.value;
            const content = promptEditor.value;
            if (!selectedFile) {
                alert("请先选择一个要保存的Prompt文件。");
                return;
            }

            savePromptBtn.disabled = true;
            savePromptBtn.textContent = '保存中...';

            const result = await updatePrompt(selectedFile, content);
            if (result) {
                alert(result.message || "保存成功！");
            }
            // No need to show alert on failure, as updatePrompt already does.

            savePromptBtn.disabled = false;
            savePromptBtn.textContent = '保存更改';
        });
    }

    // Handle navigation clicks
    navLinks.forEach(link => {
        link.addEventListener('click', function (e) {
            e.preventDefault();
            const hash = this.getAttribute('href');
            if (window.location.hash !== hash) {
                window.location.hash = hash;
            }
        });
    });

    // Handle hash changes (e.g., back/forward buttons, direct URL)
    window.addEventListener('hashchange', () => {
        navigateTo(window.location.hash);
    });

    // --- Event Delegation for dynamic content ---
    mainContent.addEventListener('click', async (event) => {
        const target = event.target;
        const button = target.closest('button'); // Find the closest button element
        if (!button) return;

        const row = button.closest('tr');
        const taskId = row ? row.dataset.taskId : null;

        if (button.matches('.view-json-btn')) {
            const card = button.closest('.result-card');
            const itemData = JSON.parse(card.dataset.item);
            const jsonContent = document.getElementById('json-viewer-content');
            jsonContent.textContent = JSON.stringify(itemData, null, 2);

            const modal = document.getElementById('json-viewer-modal');
            modal.style.display = 'flex';
            setTimeout(() => modal.classList.add('visible'), 10);
        } else if (button.matches('.run-task-btn')) {
            const taskId = button.dataset.taskId;
            button.disabled = true;
            button.textContent = '启动中...';
            await startSingleTask(taskId);
            // The auto-refresh will update the UI. For immediate feedback:
            const tasks = await fetchTasks();
            document.getElementById('tasks-table-container').innerHTML = renderTasksTable(tasks);
        } else if (button.matches('.stop-task-btn')) {
            const taskId = button.dataset.taskId;
            button.disabled = true;
            button.textContent = '停止中...';
            await stopSingleTask(taskId);
            // The auto-refresh will update the UI. For immediate feedback:
            const tasks = await fetchTasks();
            document.getElementById('tasks-table-container').innerHTML = renderTasksTable(tasks);
        } else if (button.matches('.edit-btn')) {
            const taskData = JSON.parse(row.dataset.task);

            row.classList.add('editing');
            row.innerHTML = `
                <td>
                    <label class="switch">
                        <input type="checkbox" ${taskData.enabled ? 'checked' : ''} data-field="enabled">
                        <span class="slider round"></span>
                    </label>
                </td>
                <td><input type="text" value="${taskData.task_name}" data-field="task_name"></td>
                <td><input type="text" value="${taskData.keyword}" data-field="keyword"></td>
                <td>
                    <input type="text" value="${taskData.min_price || ''}" placeholder="不限" data-field="min_price" style="width: 60px;"> -
                    <input type="text" value="${taskData.max_price || ''}" placeholder="不限" data-field="max_price" style="width: 60px;">
                </td>
                <td>
                    <label>
                        <input type="checkbox" ${taskData.personal_only ? 'checked' : ''} data-field="personal_only"> 个人闲置
                    </label>
                </td>
                <td><input type="number" value="${taskData.max_pages || 3}" data-field="max_pages" style="width: 60px;" min="1"></td>
                <td>${(taskData.ai_prompt_criteria_file || 'N/A').replace('prompts/', '')}</td>
                <td><input type="text" value="${taskData.cron || ''}" placeholder="* * * * *" data-field="cron"></td>
                <td>
                    <button class="action-btn save-btn">保存</button>
                    <button class="action-btn cancel-btn">取消</button>
                </td>
            `;

        } else if (button.matches('.delete-btn')) {
            const taskName = row.querySelector('td:nth-child(2)').textContent;
            if (confirm(`你确定要删除任务 "${taskName}" 吗?`)) {
                const result = await deleteTask(taskId);
                if (result) {
                    row.remove();
                }
            }
        } else if (button.matches('#add-task-btn')) {
            const modal = document.getElementById('add-task-modal');
            modal.style.display = 'flex';
            // Use a short timeout to allow the display property to apply before adding the transition class
            setTimeout(() => modal.classList.add('visible'), 10);
        } else if (button.matches('.save-btn')) {
            const taskNameInput = row.querySelector('input[data-field="task_name"]');
            const keywordInput = row.querySelector('input[data-field="keyword"]');
            if (!taskNameInput.value.trim() || !keywordInput.value.trim()) {
                alert('任务名称和关键词不能为空。');
                return;
            }

            const inputs = row.querySelectorAll('input[data-field]');
            const updatedData = {};
            inputs.forEach(input => {
                const field = input.dataset.field;
                if (input.type === 'checkbox') {
                    updatedData[field] = input.checked;
                } else {
                    const value = input.value.trim();
                    if (field === 'max_pages') {
                        // 确保 max_pages 作为数字发送，如果为空则默认为3
                        updatedData[field] = value ? parseInt(value, 10) : 3;
                    } else {
                        updatedData[field] = value === '' ? null : value;
                    }
                }
            });

            const result = await updateTask(taskId, updatedData);
            if (result && result.task) {
                const container = document.getElementById('tasks-table-container');
                const tasks = await fetchTasks();
                container.innerHTML = renderTasksTable(tasks);
            }
        } else if (button.matches('.cancel-btn')) {
            const container = document.getElementById('tasks-table-container');
            const tasks = await fetchTasks();
            container.innerHTML = renderTasksTable(tasks);
        }
    });

    mainContent.addEventListener('change', async (event) => {
        const target = event.target;
        // Check if the changed element is a toggle switch in the main table (not in an editing row)
        if (target.matches('.tasks-table input[type="checkbox"]') && !target.closest('tr.editing')) {
            const row = target.closest('tr');
            const taskId = row.dataset.taskId;
            const isEnabled = target.checked;

            if (taskId) {
                await updateTask(taskId, {enabled: isEnabled});
                // The visual state is already updated by the checkbox itself.
            }
        }
    });

    // --- Modal Logic ---
    const modal = document.getElementById('add-task-modal');
    if (modal) {
        const closeModalBtn = document.getElementById('close-modal-btn');
        const cancelBtn = document.getElementById('cancel-add-task-btn');
        const saveBtn = document.getElementById('save-new-task-btn');
        const form = document.getElementById('add-task-form');

        const closeModal = () => {
            modal.classList.remove('visible');
            setTimeout(() => {
                modal.style.display = 'none';
                form.reset(); // Reset form on close
            }, 300);
        };

        closeModalBtn.addEventListener('click', closeModal);
        cancelBtn.addEventListener('click', closeModal);
        modal.addEventListener('click', (event) => {
            // Close if clicked on the overlay background
            if (event.target === modal) {
                closeModal();
            }
        });

        saveBtn.addEventListener('click', async () => {
            if (form.checkValidity() === false) {
                form.reportValidity();
                return;
            }

            const formData = new FormData(form);
            const data = {
                task_name: formData.get('task_name'),
                keyword: formData.get('keyword'),
                description: formData.get('description'),
                min_price: formData.get('min_price') || null,
                max_price: formData.get('max_price') || null,
                personal_only: formData.get('personal_only') === 'on',
                max_pages: parseInt(formData.get('max_pages'), 10) || 3,
                cron: formData.get('cron') || null,
            };

            // Show loading state
            const btnText = saveBtn.querySelector('.btn-text');
            const spinner = saveBtn.querySelector('.spinner');
            btnText.style.display = 'none';
            spinner.style.display = 'inline-block';
            saveBtn.disabled = true;

            const result = await createTaskWithAI(data);

            // Hide loading state
            btnText.style.display = 'inline-block';
            spinner.style.display = 'none';
            saveBtn.disabled = false;

            if (result && result.task) {
                closeModal();
                // Refresh task list
                const container = document.getElementById('tasks-table-container');
                if (container) {
                    const tasks = await fetchTasks();
                    container.innerHTML = renderTasksTable(tasks);
                }
            }
        });
    }


    // Initial load
    refreshLoginStatusWidget();
    navigateTo(window.location.hash || '#tasks');

    // --- Global Event Listener for header/modals ---
    document.body.addEventListener('click', async (event) => {
        const target = event.target;
        const widgetUpdateBtn = target.closest('#update-login-state-btn-widget');
        const widgetDeleteBtn = target.closest('#delete-login-state-btn-widget');
        const copyCodeBtn = target.closest('#copy-login-script-btn');

        if (copyCodeBtn) {
            event.preventDefault();
            const codeToCopy = document.getElementById('login-script-code').textContent.trim();

            // 在安全上下文中使用现代剪贴板API，否则使用备用方法
            if (navigator.clipboard && window.isSecureContext) {
                navigator.clipboard.writeText(codeToCopy).then(() => {
                    copyCodeBtn.textContent = '已复制!';
                    setTimeout(() => {
                        copyCodeBtn.textContent = '复制脚本';
                    }, 2000);
                }).catch(err => {
                    console.error('无法使用剪贴板API复制文本: ', err);
                    alert('复制失败，请手动复制。');
                });
            } else {
                // 针对非安全上下文 (如HTTP) 或旧版浏览器的备用方案
                const textArea = document.createElement("textarea");
                textArea.value = codeToCopy;
                // 使文本区域不可见
                textArea.style.position = "fixed";
                textArea.style.top = "-9999px";
                textArea.style.left = "-9999px";
                document.body.appendChild(textArea);
                textArea.focus();
                textArea.select();
                try {
                    document.execCommand('copy');
                    copyCodeBtn.textContent = '已复制!';
                    setTimeout(() => {
                        copyCodeBtn.textContent = '复制脚本';
                    }, 2000);
                } catch (err) {
                    console.error('备用方案: 无法复制文本', err);
                    alert('复制失败，请手动复制。');
                }
                document.body.removeChild(textArea);
            }
        } else if (widgetUpdateBtn) {
            event.preventDefault();
            const modal = document.getElementById('login-state-modal');
            modal.style.display = 'flex';
            setTimeout(() => modal.classList.add('visible'), 10);
        } else if (widgetDeleteBtn) {
            event.preventDefault();
            if (confirm('你确定要删除登录凭证 (xianyu_state.json) 吗？删除后需要重新设置才能运行任务。')) {
                const result = await deleteLoginState();
                if (result) {
                    alert(result.message);
                    await refreshLoginStatusWidget(); // Refresh the widget UI
                    // Also refresh settings view if it's currently active
                    if (window.location.hash === '#settings' || window.location.hash === '') {
                        const statusContainer = document.getElementById('system-status-container');
                        if (statusContainer) {
                            const status = await fetchSystemStatus();
                            statusContainer.innerHTML = renderSystemStatus(status);
                        }
                    }
                }
            }
        }
    });

    // --- JSON Viewer Modal Logic ---
    const jsonViewerModal = document.getElementById('json-viewer-modal');
    if (jsonViewerModal) {
        const closeBtn = document.getElementById('close-json-viewer-btn');

        const closeModal = () => {
            jsonViewerModal.classList.remove('visible');
            setTimeout(() => {
                jsonViewerModal.style.display = 'none';
            }, 300);
        };

        closeBtn.addEventListener('click', closeModal);
        jsonViewerModal.addEventListener('click', (event) => {
            if (event.target === jsonViewerModal) {
                closeModal();
            }
        });
    }

    // --- Login State Modal Logic ---
    const loginStateModal = document.getElementById('login-state-modal');
    if (loginStateModal) {
        const closeBtn = document.getElementById('close-login-state-modal-btn');
        const cancelBtn = document.getElementById('cancel-login-state-btn');
        const saveBtn = document.getElementById('save-login-state-btn');
        const form = document.getElementById('login-state-form');
        const contentTextarea = document.getElementById('login-state-content');

        const closeModal = () => {
            loginStateModal.classList.remove('visible');
            setTimeout(() => {
                loginStateModal.style.display = 'none';
                form.reset();
            }, 300);
        };

        async function updateLoginState(content) {
            saveBtn.disabled = true;
            saveBtn.textContent = '保存中...';
            try {
                const response = await fetch('/api/login-state', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({content: content}),
                });
                if (!response.ok) {
                    const errorData = await response.json();
                    throw new Error(errorData.detail || '更新登录状态失败');
                }
                alert('登录状态更新成功！');
                closeModal();
                await refreshLoginStatusWidget(); // Refresh the widget UI
                // Also refresh settings view if it's currently active
                if (window.location.hash === '#settings') {
                    await initializeSettingsView();
                }
            } catch (error) {
                console.error('更新登录状态时出错:', error);
                alert(`更新失败: ${error.message}`);
            } finally {
                saveBtn.disabled = false;
                saveBtn.textContent = '保存';
            }
        }

        closeBtn.addEventListener('click', closeModal);
        cancelBtn.addEventListener('click', closeModal);
        loginStateModal.addEventListener('click', (event) => {
            if (event.target === loginStateModal) {
                closeModal();
            }
        });

        saveBtn.addEventListener('click', async (e) => {
            e.preventDefault();
            const content = contentTextarea.value.trim();
            if (!content) {
                alert('请粘贴从浏览器获取的JSON内容。');
                return;
            }
            await updateLoginState(content);
        });

    }
});
