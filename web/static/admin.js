const AdminView = {
  template: '#tpl-admin',
  setup() {
    const root = Vue.ref(null);
    let cleanup;
    Vue.onMounted(() => { cleanup = mountAdmin(root.value); });
    Vue.onUnmounted(() => cleanup?.());
    return { root };
  }
};

function mountAdmin(root) {
  const pageSize = 8;
  const element = id => root.querySelector('#' + id);
  const escape = value => String(value ?? '').replace(/[&<>"']/g, character => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
  }[character]));
  const date = value => value ? new Date(value * 1000).toLocaleString('zh-CN') : '—';
  const states = {
    PENDING: ['待执行', 'sky'], WAITING: ['等待中', 'amber'], PREPARING: ['准备中', 'lilac'],
    RUNNING: ['运行中', 'lilac'], DONE: ['已完成', 'mint'], FAILED: ['失败', 'rose'],
    SKIPPED: ['已跳过', 'neutral'], CANCELLED: ['已取消', 'neutral'], SUSPENDED: ['已挂起', 'amber']
  };
  const controls = {start: '启动', pause: '暂停', resume: '恢复', stop: '停止', restart: '重启'};
  const operationNames = {
    ...controls, cancel: '取消任务', retry: '重试任务', priority: '调整优先级',
    redeliver: '重投事件', browser_settings: '更新浏览器设置', browser_quota: '更新小时额度',
    series_publish: '发布系列审核'
  };
  const serviceLabels = {
    consumer: ['任务服务', 'Event Consumer', 'sky'],
    browser: ['浏览器服务', 'Browser Service', 'lilac']
  };
  let offset = 0;
  let items = [];
  let total = 0;
  let filters = {resource: 'workflows', status: '', task_id: ''};
  let listingRequest = 0;
  let busy = false;
  let refreshing = false;
  let loginJob = null;
  let balance = null;
  let usage = null;
  let usageRequest = 0;
  let selectedHour = null;
  const quotaForm = element('quota-form');
  const quotaCurve = element('quota-curve');
  const quotaTimeline = element('quota-timeline');
  let quotaDraft = null;
  let quotaScale = 60;
  let drag = null;

  function notice(message, error = false) {
    element('notice').textContent = message;
    element('notice').toggleAttribute('data-error', error);
  }

  async function api(path, method = 'GET', body) {
    const response = await fetch(path, {
      method,
      headers: {'Content-Type': 'application/json'},
      body: body === undefined ? undefined : JSON.stringify(body)
    });
    const data = await response.json();
    if (!response.ok) throw Error(data.error || response.statusText);
    return data;
  }

  async function action(button, operation) {
    if (busy) return;
    busy = true;
    if (button) button.disabled = true;
    try {
      const result = await operation();
      await load();
      notice(result?.message || '操作成功');
    } catch (error) {
      notice(error.message, true);
      if (element('priority-dialog').open) element('priority-error').textContent = error.message;
    } finally {
      busy = false;
      if (button) button.disabled = false;
    }
  }

  function recordStatus(row, resource) {
    return row.status || row.outcome || (resource === 'audit' ? '管理操作' : '未知状态');
  }

  function metrics(values) {
    return values.map(([label, value]) =>
      '<div class="metric"><dt>' + label + '</dt><dd>' + escape(value) + '</dd></div>'
    ).join('');
  }

  async function loadServices() {
    const {services, browser} = await api('/api/admin/services');
    const service = services.find(item => item.name === 'browser');
    const sessionOpen = service?.alive && service.desired !== 'stop' && browser.session_open;
    const sessionBlocked = {waiting_login: '等待登录', waiting_verification: '等待验证'}[service?.state];
    const sessionHealthy = sessionOpen && browser.session_logged_in === true && !sessionBlocked;
    const deadline = browser.next_allowed_at;
    const now = Date.now() / 1000;
    balance = {service, deadline, readAt: now};
    renderBalance();
    if (!element('services').contains(document.activeElement) || busy) {
      element('services').innerHTML = services.map(service => {
        const name = service.name;
        const [label, title, tone] = serviceLabels[name];
        return `
          <article class="card ${name}-card">
            <div class="card-top"><span class="pill ${tone}">${label}</span>
            <span class="pill ${name === 'browser' ? sessionHealthy ? 'mint' : 'neutral' : service.alive ? 'mint' : 'neutral'}">
              ${name === 'browser' && sessionHealthy ? '会话正常' : service.alive ? '在线' : '离线'}
            </span></div>
            <h3>${title}</h3>
            <dl class="metrics">${metrics([
              ['状态', service.state], ['进程', service.pid || '—'],
              ['心跳', date(service.heartbeat)], ['当前工作', service.current_id || '—']
            ])}</dl>
            ${service.message ? '<p class="hint">' + escape(service.message) + '</p>' : ''}
            <div class="actions">${Object.entries(controls).map(([nameAction, label]) => `
              <button data-service="${name}" data-action="${nameAction}" aria-label="${label} ${title}">${label}</button>
            `).join('')}</div>
          </article>
        `;
      }).join('');
    }
    const sessionBadge = element('session-health');
    sessionBadge.className = 'pill ' + (sessionHealthy ? 'mint' : sessionOpen ? 'amber' : 'neutral');
    sessionBadge.textContent = sessionHealthy ? '已登录' : sessionOpen
      ? sessionBlocked || (browser.session_logged_in === false ? '登录待处理' : '登录未确认')
      : '浏览器未打开';
    if (!element('settings').children.length) {
      const labels = {
        interval_min: '最短访问间隔', interval_max: '最长访问间隔',
        backoff_base: '失败退避起点', backoff_max: '最大失败退避'
      };
      element('settings').innerHTML = Object.entries(labels).map(([key, label]) => `
        <label>${label}<input name="${key}" type="number" min="1" max="86400" step="any" value="${escape(browser[key])}" required></label>
      `).join('');
      element('save').disabled = false;
    }
    element('cooldown').innerHTML = metrics([
      ['上次检查结果', browser.logged_in == null ? '未检查' : browser.logged_in ? '已登录' : '未登录'],
      ['检查时间', date(browser.login_checked_at)], ['连续失败', browser.failures ?? '—']
    ]);
    if (loginJob) {
      const jobId = loginJob;
      const task = await api('/api/jobs/' + encodeURIComponent(jobId));
      if (loginJob !== jobId) return;
      const terminal = ['DONE', 'FAILED', 'SKIPPED', 'CANCELLED'].includes(task.status);
      const label = states[task.status]?.[0] || task.status;
      const result = task.error || (typeof task.result === 'boolean' ? (task.result ? '操作成功' : '未登录') : '');
      element('login-result').textContent = result ? `${label} · ${result}` : label;
      if (terminal) loginJob = null;
    }
  }

  function parseJSON(value) {
    try { return JSON.parse(value); } catch { return value; }
  }

  function renderBalance() {
    if (!balance) return;
    const {service, deadline, readAt} = balance;
    const now = Date.now() / 1000;
    const seconds = Math.max(0, Math.ceil(deadline - now));
    const stale = !service || now - readAt > 15;
    let status = '可接收访问';
    if (stale) status = '状态暂不可用';
    else if (!service.alive || service.desired === 'stop') status = '服务离线';
    else if (service.state === 'waiting_login') status = '等待登录';
    else if (service.state === 'waiting_verification') status = '等待人工验证';
    else if (service.paused) status = '服务已暂停';
    else if (service.state === 'starting') status = '服务启动中';
    else if (service.current_id) status = '正在执行命令';
    else if (seconds) status = '冷却中';
    else if (service.state === 'hourly_limit') status = '本小时额度已用尽';
    element('balance-state').textContent = !stale && seconds ? `${status} · 冷却剩余 ${seconds} 秒` : status;
  }

  function usageLabel(hour) {
    return new Intl.DateTimeFormat('zh-CN', {timeZone: usage.timezone, hour: '2-digit',
      minute: '2-digit', hourCycle: 'h23', timeZoneName: 'short'}).format(hour.start * 1000);
  }

  function showUsageHour(index) {
    selectedHour = index;
    const hour = usage.hours[index];
    const counts = hour.kinds;
    const used = hour.used == null ? (hour.start > Date.now() / 1000 ? '尚未开始' : '无记录') : `已使用 ${hour.used} 次`;
    const allowed = hour.allowed == null ? '额度未配置' : `允许 ${hour.allowed} 次`;
    const remaining = hour.used != null && hour.allowed != null && !hour.partial
      ? ` · 剩余 ${Math.max(0, hour.allowed - hour.used)} 次` : '';
    const other = (hour.used || 0) - (counts.video || 0) - (counts.actor || 0);
    const breakdown = hour.used == null ? '' : ` · 影片 ${counts.video || 0} / 演员 ${counts.actor || 0} / 其他 ${other}`;
    element('usage-hour-detail').textContent = `${usageLabel(hour)} · ${allowed} · ${used}${remaining}${breakdown}${hour.partial ? '（仅部分时段）' : ''}`;
    for (const button of element('usage-chart').children) {
      button.setAttribute('aria-pressed', String(Number(button.dataset.usageHour) === index));
    }
  }

  async function loadUsage() {
    const requestId = ++usageRequest;
    const dateInput = element('usage-date');
    const query = dateInput.value ? '?' + new URLSearchParams({date: dateInput.value}) : '';
    const next = await api('/api/admin/browser/usage' + query);
    if (requestId !== usageRequest) return;
    dateInput.value = next.date;
    dateInput.max = next.today;
    if (JSON.stringify(usage) === JSON.stringify(next)) return;
    const chart = element('usage-chart');
    const focused = chart.contains(document.activeElement);
    if (usage?.date !== next.date) selectedHour = null;
    usage = next;
    const observed = usage.hours.filter(hour => hour.used != null);
    const total = observed.reduce((sum, hour) => sum + hour.used, 0);
    const assigned = usage.hours.filter(hour => hour.allowed != null);
    const allowedTotal = assigned.reduce((sum, hour) => sum + hour.allowed, 0);
    const scale = Math.max(1, ...usage.hours.map(hour => Math.max(hour.used || 0, hour.allowed || 0)));
    element('usage-scale').textContent = `纵轴 0–${scale} 次/小时`;
    const allowance = assigned.length ? `日额度 ${allowedTotal} 次${assigned.length < usage.hours.length ? '（已配置时段）' : ''}` : '当天未配置小时额度';
    element('usage-total').textContent = `${allowance} · ${observed.length ? `已记录 ${total} 次` : '暂无使用数据'}`;
    chart.style.setProperty('--hours', usage.hours.length);
    chart.innerHTML = usage.hours.map((hour, index) => {
      const label = `${usageLabel(hour)} · ${hour.allowed == null ? '额度未配置' : `允许 ${hour.allowed} 次`} · ${hour.used == null ? '暂无数据' : `已使用 ${hour.used} 次`}`;
      return `<button type="button" class="usage-hour" data-usage-hour="${index}" aria-label="${escape(label)}">
        <span class="usage-bar" aria-hidden="true">
          <span class="usage-allowed" style="height:${(hour.allowed || 0) / scale * 100}%"></span>
          <span class="usage-used" style="height:${(hour.used || 0) / scale * 100}%"></span>
        </span><span aria-hidden="true">${escape(hour.label.slice(0, 2))}</span></button>`;
    }).join('');
    const latest = usage.hours.filter(hour => hour.start <= Date.now() / 1000).length - 1;
    showUsageHour(selectedHour ?? Math.max(0, latest));
    if (focused) chart.children[selectedHour].focus({preventScroll: true});
    const scheduled = usage.effective_at > Date.now() / 1000
      ? `新曲线将于 ${usageLabel({start: usage.effective_at})} 生效。 ` : '';
    element('usage-note').textContent = `${scheduled}太平洋时间。每次放行的浏览器命令或后台刷新计一次。点击小时查看数值和用途。`;
    if (!quotaDraft) {
      quotaDraft = usage.quota ? {...usage.quota} : {peak_hour: 14, peak_limit: 30, spread: 4};
      quotaScale = Math.max(60, Math.ceil(quotaDraft.peak_limit * 1.25));
      renderQuota();
    }
  }

  const wrapHour = hour => (hour % 24 + 24) % 24;

  function renderQuota() {
    const {peak_hour: peak, peak_limit: limit, spread} = quotaDraft;
    const valueAt = offset => limit * Math.exp(-0.5 * (offset / spread) ** 2);
    const x = offset => 300 + 560 * offset / 24;
    const y = amount => 146 - 126 * amount / quotaScale;
    const setHandle = (id, offset) => {
      element(id).setAttribute('cx', x(offset));
      element(id).setAttribute('cy', y(valueAt(offset)));
    };
    const points = Array.from({length: 97}, (_, index) => {
      const offset = index / 4 - 12;
      return `${x(offset)},${y(valueAt(offset))}`;
    });
    element('quota-line').setAttribute('d', 'M' + points.join(' L'));
    setHandle('quota-peak', 0);
    setHandle('quota-width', spread);
    quotaTimeline.tabIndex = 0;
    quotaTimeline.removeAttribute('aria-disabled');
    quotaTimeline.setAttribute('aria-valuenow', peak);
    const peakTime = `${String(peak).padStart(2, '0')}:00`;
    quotaTimeline.setAttribute('aria-valuetext', peakTime);
    quotaTimeline.innerHTML = Array.from({length: 24}, (_, index) => {
      const hour = String(wrapHour(peak + index - 12)).padStart(2, '0');
      const tone = index === 12 ? 'is-peak is-major' : index % 3 === 0 ? 'is-major' : '';
      return `<span class="quota-tick ${tone}" aria-hidden="true"><span>${hour}</span></span>`;
    }).join('');
    for (const key of ['peak_hour', 'peak_limit', 'spread']) {
      quotaForm.elements[key].value = quotaDraft[key];
    }
    const total = Array.from({length: 24}, (_, hour) => Math.round(valueAt(hour - 12))).reduce((a, b) => a + b, 0);
    element('quota-preview').textContent = `预览 · 峰值 ${peakTime} / ${limit} 次 · 全天合计 ${total} 次`;
  }

  async function loadListing() {
    const requestId = ++listingRequest;
    const {resource, status, task_id} = filters;
    const query = new URLSearchParams({limit: pageSize, offset, status, task_id});
    const data = await api('/api/admin/' + resource + '?' + query);
    if (requestId !== listingRequest) return;
    items = data.items;
    total = data.total;
    element('total').textContent = items.length ? `第 ${offset + 1}–${offset + items.length} 条，共 ${total} 条` : `共 ${total} 条`;
    element('previous').disabled = offset === 0;
    element('next').disabled = offset + items.length >= total;
    element('rows').innerHTML = items.map((row, index) => {
      const checkpoint = parseJSON(row.checkpoint_json) || {};
      const status = recordStatus(row, resource);
      const [label, tone] = states[status] || [status, 'neutral'];
      const canManage = resource === 'workflows' && !['ltx', 'ltx_preview'].includes(row.kind);
      const canDelete = resource === 'workflows' && ['DONE','FAILED','SKIPPED','CANCELLED','SUSPENDED'].includes(status);
      const canCancel = canManage && ['PENDING', 'RUNNING', 'WAITING', 'PREPARING'].includes(status);
      const failedItems = Array.isArray(checkpoint.items) && checkpoint.items.some(item => !item.ok);
      const canRetry = canManage && (['FAILED', 'SKIPPED', 'CANCELLED'].includes(status)
        || (status === 'DONE' && row.kind === 'pipeline_rescrape' && failedItems));
      const canRedeliver = resource === 'events' && (!row.acknowledged_at || ['FAILED', 'SKIPPED'].includes(row.outcome));
      const title = row.code || operationNames[row.action] || row.kind || row.command_id || '记录 #' + row.id;
      return `<article class="card record">
        <div class="card-top"><span class="pill ${tone}">${escape(label)}</span><button data-detail="${index}">详情</button></div>
        <h3 class="record-title">${escape(title)}</h3>
        <p class="muted">#${escape(row.id)}</p>
        <div class="record-meta muted">
          ${row.kind === 'actor' ? '<span>演员刮削</span>' : ''}
          ${row.kind === 'rename_files' ? '<span>文件重命名</span>' : ''}
          ${row.kind === 'series_translate' ? '<span>系列翻译</span>' : ''}
          <span>${date(row.created_at || row.finished_at)}</span>
          ${checkpoint.phase ? '<span>' + escape(checkpoint.phase) + '</span>' : ''}
          ${resource === 'events' ? '<span>' + (row.acknowledged_at ? '已确认' : '待确认') + '</span>' : ''}
        </div>
        ${row.error ? '<p class="record-error muted">' + escape(row.error) + '</p>' : ''}
        ${canManage || canRedeliver || canDelete ? `<div class="actions">
          ${canDelete ? `<button data-task="${index}" data-action="delete">删除</button>` : ''}
          ${canCancel ? `<button data-task="${index}" data-action="cancel">取消</button>` : ''}
          ${canRetry ? `<button data-task="${index}" data-action="retry">重试</button>` : ''}
          ${canManage ? `<button data-task="${index}" data-action="priority">优先级 ${escape(row.priority ?? 0)}</button>` : ''}
          ${canRedeliver ? `<button data-event="${escape(row.id)}">重投</button>` : ''}
        </div>` : ''}
      </article>`;
    }).join('') || '<p class="card empty">没有符合条件的记录。</p>';
  }

  async function loadActivity() {
    const {items: recent} = await api('/api/admin/audit?limit=5');
    element('activity').innerHTML = recent.map(row => {
      const [label, title, tone] = Object.hasOwn(serviceLabels, row.target) ? serviceLabels[row.target] : [];
      return `<li>
        <div class="activity-body">
          ${label ? `<span class="pill ${tone}">${label}</span> <strong class="activity-target">${title}</strong>` : `<strong class="activity-target">${escape(row.target)}</strong>`}
          <p class="activity-action">${escape(operationNames[row.action] || row.action)}</p>
        </div>
        <time class="muted" datetime="${new Date(row.created_at * 1000).toISOString()}">${date(row.created_at)}</time>
      </li>`;
    }).join('') || '<li class="muted">暂无管理操作。</li>';
  }

  async function load(background = false) {
    const requests = [loadServices(), loadActivity(), loadUsage()];
    if (!background || !element('rows').contains(document.activeElement)) requests.push(loadListing());
    await Promise.all(requests);
  }

  function applyFilters() {
    const resource = element('resource').value;
    filters = {
      resource,
      status: element('status').disabled ? '' : element('status').value.trim(),
      task_id: resource === 'audit' ? '' : element('task-filter').value.trim()
    };
    offset = 0;
    loadListing().catch(error => notice(error.message, true));
  }

  function showDetail(index) {
    const expanded = {...items[index]};
    for (const key of Object.keys(expanded)) {
      if (key.endsWith('_json') && expanded[key]) expanded[key] = parseJSON(expanded[key]);
    }
    element('detail-body').textContent = JSON.stringify(expanded, null, 2);
    element('detail').showModal();
  }

  root.addEventListener('click', event => {
    const button = event.target.closest('button');
    if (!button) return;
    if (button.dataset.usageHour !== undefined) showUsageHour(Number(button.dataset.usageHour));
    if (button.dataset.service) {
      action(button, () => api('/api/admin/services/' + button.dataset.service + '/control', 'POST', {action: button.dataset.action}));
    }
    if (button.dataset.browser) {
      action(button, async () => {
        const data = await api('/api/admin/browser/' + button.dataset.browser, 'POST', {});
        loginJob = data.job_id;
        element('login-result').textContent = '任务已入队 ' + loginJob;
      });
    }
    if (button.dataset.detail !== undefined) showDetail(Number(button.dataset.detail));
    if (button.dataset.task !== undefined) {
      const task = items[Number(button.dataset.task)];
      if (button.dataset.action === 'delete') {
        if (window.confirm('删除这条任务记录？')) {
          action(button, () => api('/api/admin/workflows/' + encodeURIComponent(task.id), 'DELETE'));
        }
      } else if (button.dataset.action === 'priority') {
        element('priority-form').dataset.task = task.id;
        element('priority-value').value = task.priority ?? 0;
        element('priority-error').textContent = '';
        element('priority-dialog').showModal();
      } else {
        action(button, () => api('/api/admin/workflows/' + encodeURIComponent(task.id) + '/' + button.dataset.action, 'POST', {}));
      }
    }
    if (button.dataset.event) {
      action(button, () => api('/api/admin/events/' + button.dataset.event + '/redeliver', 'POST', {}));
    }
  });

  element('settings').onsubmit = event => {
    event.preventDefault();
    if (!element('settings').children.length) return;
    const values = Object.fromEntries([...new FormData(element('settings'))].map(([key, value]) => [key, Number(value)]));
    action(element('save'), () => api('/api/admin/browser/settings', 'PATCH', values));
  };
  element('priority-form').onsubmit = event => {
    event.preventDefault();
    const taskId = event.currentTarget.dataset.task;
    const priority = Number(element('priority-value').value);
    action(event.submitter, async () => {
      await api('/api/admin/workflows/' + encodeURIComponent(taskId) + '/priority', 'POST', {priority});
      element('priority-dialog').close();
    });
  };
  element('jump-to-records').onclick = () => element('records-heading').scrollIntoView({block: 'start'});
  element('priority-cancel').onclick = () => element('priority-dialog').close();
  element('filters').onsubmit = event => { event.preventDefault(); applyFilters(); };
  element('resource').onchange = () => {
    const resource = element('resource').value;
    element('status').disabled = !['workflows', 'commands'].includes(resource);
    element('status').value = '';
    element('task-filter').disabled = resource === 'audit';
    applyFilters();
  };
  async function changePage(nextOffset) {
    const previousOffset = offset;
    offset = nextOffset;
    element('previous').disabled = true;
    element('next').disabled = true;
    try { await loadListing(); }
    catch (error) {
      if (offset === nextOffset) {
        offset = previousOffset;
        element('previous').disabled = offset === 0;
        element('next').disabled = offset + items.length >= total;
      }
      notice(error.message, true);
    }
  }
  element('previous').onclick = () => changePage(Math.max(0, offset - pageSize));
  element('next').onclick = () => changePage(offset + pageSize);
  element('usage-date').onchange = () => loadUsage().catch(error => notice(error.message, true));
  quotaForm.onchange = () => {
    if (!quotaDraft || !quotaForm.checkValidity()) return;
    for (const key of ['peak_hour', 'peak_limit', 'spread']) quotaDraft[key] = quotaForm.elements[key].valueAsNumber;
    quotaScale = Math.max(60, Math.ceil(quotaDraft.peak_limit * 1.25));
    renderQuota();
  };
  quotaForm.onsubmit = event => {
    event.preventDefault();
    action(event.submitter, async () => {
      const [peak_hour, peak_limit, spread] = ['peak_hour', 'peak_limit', 'spread']
        .map(key => quotaForm.elements[key].valueAsNumber);
      await api('/api/admin/browser/quota', 'PUT', {peak_hour, peak_limit, spread});
      return {message: '额度曲线已保存，下个整点生效'};
    });
  };
  quotaCurve.onpointerdown = event => {
    if (!quotaDraft || !event.target.dataset.handle || drag || event.button !== 0) return;
    drag = {handle: event.target.dataset.handle, pointer: event.pointerId};
    quotaCurve.setPointerCapture(event.pointerId);
  };
  quotaCurve.onpointermove = event => {
    if (drag?.pointer !== event.pointerId) return;
    const point = new DOMPoint(event.clientX, event.clientY).matrixTransform(quotaCurve.getScreenCTM().inverse());
    if (drag.handle === 'peak') {
      quotaDraft.peak_limit = Math.max(1, Math.min(3600, quotaScale, Math.round((146 - point.y) * quotaScale / 126)));
    } else {
      quotaDraft.spread = Math.max(0.5, Math.min(12, Math.round((point.x - 300) * 48 / 560) / 2));
    }
    renderQuota();
  };
  quotaCurve.onlostpointercapture = () => { drag = null; };
  quotaTimeline.onpointerdown = event => {
    if (!quotaDraft || drag || event.button !== 0) return;
    drag = {handle: 'timeline', pointer: event.pointerId, start: event.clientX,
      peak: quotaDraft.peak_hour, hourWidth: quotaTimeline.getBoundingClientRect().width / 24};
    quotaTimeline.setPointerCapture(event.pointerId);
  };
  quotaTimeline.onpointermove = event => {
    if (drag?.handle !== 'timeline' || drag.pointer !== event.pointerId || !drag.hourWidth) return;
    const peak = wrapHour(drag.peak - Math.round((event.clientX - drag.start) / drag.hourWidth));
    if (peak !== quotaDraft.peak_hour) {
      quotaDraft.peak_hour = peak;
      renderQuota();
    }
  };
  quotaTimeline.onlostpointercapture = () => { drag = null; };
  quotaTimeline.onkeydown = event => {
    if (!quotaDraft) return;
    const steps = {ArrowRight: 1, ArrowUp: 1, ArrowLeft: -1, ArrowDown: -1, PageUp: 3, PageDown: -3};
    if (event.key in steps) quotaDraft.peak_hour = wrapHour(quotaDraft.peak_hour + steps[event.key]);
    else if (event.key === 'Home') quotaDraft.peak_hour = 0;
    else if (event.key === 'End') quotaDraft.peak_hour = 23;
    else return;
    event.preventDefault();
    renderQuota();
  };
  load().then(() => notice('状态已读取')).catch(error => notice(error.message, true));
  const timer = setInterval(async () => {
    if (busy || refreshing || document.hidden || root.querySelector('dialog[open]')) return;
    refreshing = true;
    try { await load(true); }
    catch (error) { notice(error.message, true); }
    finally { refreshing = false; }
  }, 5000);
  const balanceTimer = setInterval(renderBalance, 1000);
  return () => { clearInterval(timer); clearInterval(balanceTimer); };
}
