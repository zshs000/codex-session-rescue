"use strict";

const $ = (id) => document.getElementById(id);
const state = { token: "", tab: "sessions", sessions: [], total: 0, selected: null, family: null, inspection: null,
  checked: new Set(), focused: null, includeChildren: true, busy: false, backups: [], querySeq: 0, gateway: null,
  gatewayIncludeChildren: true, gatewayUpstream: "", gatewayProvider: "" };
let searchTimer, toastTimer;

function h(tag, props = {}, ...children) {
  const element = document.createElement(tag);
  for (const [key, value] of Object.entries(props)) {
    if (value === undefined || value === null) continue;
    if (key === "class") element.className = value;
    else if (key.startsWith("on") && typeof value === "function") element.addEventListener(key.slice(2), value);
    else if (["checked", "disabled", "value", "selected"].includes(key)) element[key] = value;
    else element.setAttribute(key, String(value));
  }
  for (const child of children.flat(Infinity)) {
    if (child === null || child === undefined || child === false) continue;
    element.append(child instanceof Node ? child : document.createTextNode(String(child)));
  }
  return element;
}
function setContent(...children) {
  $("content").replaceChildren(...children.flat(Infinity).filter((child) => child !== null && child !== undefined && child !== false));
}

function num(value) { return value === null || value === undefined ? "—" : Number(value).toLocaleString("zh-CN"); }
function size(value) { return value >= 1048576 ? `${(value / 1048576).toFixed(1)} MB` : `${Math.round(value / 1024)} KB`; }
function date(value) {
  if (!value) return "";
  const parsed = typeof value === "number" ? new Date(value > 1e12 ? value : value * 1000) : new Date(value);
  return Number.isNaN(parsed.valueOf()) ? "" : new Intl.DateTimeFormat("zh-CN", { month: "2-digit", day: "2-digit", hour: "2-digit", minute: "2-digit", hour12: false }).format(parsed);
}
function project(cwd) { return (cwd || "").replace(/\\\\\?\\/, "").split(/[\\/]/).filter(Boolean).slice(-2).join(" / ") || "本地会话"; }
function badge(label, color = "gray") { return h("span", { class: `badge ${color}` }, label); }
function button(label, callback, className = "secondary", extra = {}) {
  return h("button", { class: className, onclick: callback, disabled: state.busy, ...extra }, label);
}
function toast(message) {
  clearTimeout(toastTimer); $("toast").textContent = message; $("toast").classList.remove("hidden");
  toastTimer = setTimeout(() => $("toast").classList.add("hidden"), 3200);
}
function notice(message, success = false, extras = []) {
  $("notice").className = `notice${success ? " success" : ""}`;
  $("notice").replaceChildren(document.createTextNode(message), ...extras);
}
function clearNotice() { $("notice").classList.add("hidden"); $("notice").replaceChildren(); }
async function copy(text) {
  try { await navigator.clipboard.writeText(text); toast("已复制"); }
  catch { notice("无法自动复制，请选中页面中的文本手动复制。"); }
}
async function api(path, data) {
  const options = { headers: { "X-Session-Token": state.token } };
  if (data !== undefined) { options.method = "POST"; options.headers["Content-Type"] = "application/json"; options.body = JSON.stringify(data); }
  const response = await fetch(path, options);
  const result = await response.json();
  if (!response.ok) throw new Error(result.error || "请求未完成");
  return result;
}

function updateControls() {
  $("exitButton").disabled = state.busy; $("sessionsTab").disabled = state.busy; $("backupsTab").disabled = state.busy;
  $("searchInput").disabled = state.busy; $("kindFilter").disabled = state.busy; $("archiveFilter").disabled = state.busy;
}
async function runJob(path, data, done) {
  if (state.busy) return;
  clearNotice(); state.busy = true; updateControls(); render(); renderSessions();
  $("jobPanel").classList.remove("hidden"); $("jobMessage").textContent = "正在启动"; $("jobProgress").value = 0;
  try {
    let job = await api(path, data);
    while (true) {
      $("jobMessage").textContent = job.message || "正在处理"; $("jobProgress").value = job.progress || 0;
      if (job.state === "failed") throw new Error(job.error || "操作未完成");
      if (job.state === "succeeded") { await done(job.result); break; }
      await new Promise((resolve) => setTimeout(resolve, 500));
      job = await api(`/api/jobs/${job.id}`);
    }
  } catch (error) {
    notice(error.message || "操作未完成，请刷新后重试。");
    if (path === "/api/clean") { state.inspection = null; state.checked.clear(); }
  } finally {
    state.busy = false; $("jobPanel").classList.add("hidden"); updateControls(); render(); renderSessions();
  }
}

async function search(more = false) {
  const sequence = ++state.querySeq;
  if (!more) { state.sessions = []; renderSessions(true); }
  const params = new URLSearchParams({ q: $("searchInput").value, kind: $("kindFilter").value,
    archived: String($("archiveFilter").checked), offset: String(more ? state.sessions.length : 0), limit: "40" });
  try {
    const data = await api(`/api/sessions?${params}`);
    if (sequence !== state.querySeq) return;
    state.sessions = more ? [...state.sessions, ...data.items] : data.items; state.total = data.total;
    $("resultCount").textContent = `${num(data.total)} 条`; renderSessions();
  } catch (error) {
    if (sequence !== state.querySeq) return;
    $("resultCount").textContent = "读取失败";
    $("sessionList").replaceChildren(h("div", { class: "list-empty" }, error.message));
  }
}
function renderSessions(loading = false) {
  const list = $("sessionList"), scroll = list.scrollTop;
  if (loading) { list.replaceChildren(h("div", { class: "list-empty" }, "正在读取本地会话…")); return; }
  if (!state.sessions.length) { list.replaceChildren(h("div", { class: "list-empty" }, "没有找到匹配的会话。", h("br"), "试试粘贴完整的会话 ID。")); }
  else list.replaceChildren(...state.sessions.map((item) => h("button", {
    class: `session-card${state.selected?.id === item.id ? " selected" : ""}`, disabled: state.busy,
    onclick: () => selectThread(item.id), "aria-label": `${item.title}，${item.id}`,
  }, h("span", { class: "row-top" }, h("span", { class: "session-title" }, item.is_subagent ? `${item.agent_nickname || "子代理"} · ${item.title}` : item.title),
    item.has_error_record ? h("i", { class: "status-dot", title: "曾记录此加密错误" }) : null),
  h("span", { class: "session-meta" }, h("span", {}, item.archived ? "已归档" : project(item.cwd)), h("span", {}, date(item.updated_at))),
  h("span", { class: "session-meta" }, h("span", {}, item.id.slice(0, 18) + "…"), item.child_count ? h("span", {}, `${item.child_count} 个子代理`) : null))));
  list.scrollTop = scroll;
  $("loadMore").classList.toggle("hidden", state.sessions.length >= state.total || !state.sessions.length);
  $("loadMore").disabled = state.busy;
}
async function selectThread(identity) {
  if (state.busy) return;
  clearNotice(); state.inspection = null; state.checked.clear(); state.focused = identity; state.gateway = null;
  try {
    const family = await api(`/api/sessions/${encodeURIComponent(identity)}`);
    state.family = family; state.selected = family.thread; state.includeChildren = true;
    state.tab = "sessions"; updateTabs(); render(); renderSessions();
    await refreshGateway(identity);
    $("workspace").scrollIntoView({ behavior: "smooth", block: "start" });
  } catch (error) { notice(error.message); }
}

function updateTabs() {
  $("layout").classList.toggle("mode-backups", state.tab === "backups");
  $("sessionsTab").classList.toggle("active", state.tab === "sessions");
  $("backupsTab").classList.toggle("active", state.tab === "backups");
}
async function showBackups() {
  state.tab = "backups"; updateTabs(); render();
  try {
    const data = await api("/api/backups"); state.backups = data.items;
    const count = data.items.filter((item) => item.can_recover).length;
    $("pendingCount").textContent = String(count); $("pendingCount").classList.toggle("hidden", !count);
    render();
  } catch (error) { notice(error.message); }
}
function render() {
  if (state.tab === "backups") renderBackups();
  else if (!state.selected) renderWelcome();
  else renderThread();
}
function renderWelcome() {
  setContent(h("section", { class: "welcome" },
    h("div", { class: "welcome-visual", "aria-hidden": "true" }, h("div", { class: "welcome-window" },
      h("div", { class: "window-dots" }, h("i"), h("i"), h("i")), h("div", { class: "visual-line" }), h("div", { class: "visual-line short" }),
      h("div", { class: "visual-check" }, h("span", {}, "✓"), "历史与索引同步"))),
    h("h1", {}, "选择一条需要修复的会话"),
    h("p", {}, "在左侧搜索标题，或直接粘贴 codex resume 命令。先查看结构与关联，再决定清理范围。"),
    h("div", { class: "steps" },
      h("article", { class: "step" }, h("span", { class: "step-number" }, "01 / 定位"), h("h3", {}, "找到具体对话"), h("p", {}, "按 ID、标题、内容摘要或项目路径查找。")),
      h("article", { class: "step" }, h("span", { class: "step-number" }, "02 / 检查"), h("h3", {}, "看清关联与结构"), h("p", {}, "一起检查主会话、子代理及回滚形成的历史分段。")),
      h("article", { class: "step" }, h("span", { class: "step-number" }, "03 / 修复"), h("h3", {}, "先备份，再清理"), h("p", {}, "同步历史索引，保存清理前快照，支持检查后回滚。"))),
    h("p", { class: "welcome-note" }, h("span", {}, "ⓘ"), "用于处理 invalid_encrypted_content。加密内容本身是正常记录，不会自动清理所有会话。")));
}

function stat(label, value, unit, foot) {
  return h("div", { class: "stat" }, h("div", { class: "stat-label" }, label),
    h("div", { class: `stat-value${typeof value === "string" ? " textual" : ""}` }, typeof value === "string" ? value : num(value), h("small", {}, unit)),
    h("div", { class: "stat-foot" }, foot));
}
function orderedNodes(nodes) {
  const groups = new Map(), byId = new Map(nodes.map((node) => [node.id, node]));
  for (const node of nodes) { if (!groups.has(node.parent_id)) groups.set(node.parent_id, []); groups.get(node.parent_id).push(node); }
  const result = [], seen = new Set();
  function walk(identity, depth) { if (seen.has(identity)) return; const node = byId.get(identity); if (!node) return; seen.add(identity); result.push({ ...node, depth }); for (const child of groups.get(identity) || []) walk(child.id, depth + 1); }
  walk(state.selected.id, 0); return result;
}
function nodeBadge(node) {
  if (!state.inspection || node.encrypted_blocks === undefined) return badge("待检查");
  if (node.ready) return node.error_evidence?.found ? badge("有报错记录", "amber") : badge("可选择清理", "green");
  if (node.encrypted_blocks === 0) return badge(node.cleaned ? "已清理" : "无需清理", "green");
  return badge(node.reason?.includes("占用") ? "会话被占用" : "暂不能自动处理", "amber");
}
function treePanel(nodes) {
  return h("section", { class: "panel" },
    h("div", { class: "panel-header" }, h("div", {}, h("h2", {}, "会话与子代理"), h("p", {}, "选择实际报错、需要处理的会话。点击名称可查看对应文件结构。")),
      state.inspection ? h("div", { class: "panel-actions" },
        button("仅当前", () => { state.checked = new Set(nodes.filter((n) => n.id === state.selected.id && n.ready).map((n) => n.id)); render(); }, "quiet"),
        button("选择全部可处理项", () => { state.checked = new Set(nodes.filter((n) => n.ready).map((n) => n.id)); render(); }, "quiet")) : null),
    h("div", { class: "tree-header" }, h("span", {}, "会话"), h("span", {}, "加密块"), h("span", {}, "检查结果")),
    h("div", { class: "tree-list" }, orderedNodes(nodes).map((node) => h("div", { class: `tree-row depth-${Math.min(node.depth, 6)}${state.focused === node.id ? " focused" : ""}` },
      h("input", { type: "checkbox", checked: state.checked.has(node.id), disabled: state.busy || !node.ready, "aria-label": `选择 ${node.title}`,
        onchange: (event) => { event.target.checked ? state.checked.add(node.id) : state.checked.delete(node.id); render(); } }),
      h("button", { class: "tree-name", disabled: state.busy, onclick: () => { state.focused = node.id; render(); }, title: node.title },
        h("span", { class: "node-title" }, node.id === state.selected.id ? node.title : `${node.agent_nickname || "子代理"} · ${node.agent_path?.split("/").pop() || node.title}`),
        h("span", { class: "node-caption" }, node.depth ? "子代理 · " : "当前会话 · ", node.id.slice(0, 18), "…")),
      h("div", { class: "node-count" }, num(node.encrypted_blocks), node.encrypted_blocks != null ? h("small", {}, "个") : null),
      h("div", { class: "node-status" }, nodeBadge(node), node.reason && node.encrypted_blocks !== 0 ? h("small", { title: node.reason }, node.reason) : null)))),
    h("div", { class: "tree-hint" }, state.inspection ? "正常聊天、工具记录和可读历史会保留。子代理转存到父会话中的加密块也会检查。" : "点击上方「检查会话」，读取文件结构并核对历史索引。"));
}
function segmentPanel(node) {
  if (!node || !state.inspection) return null;
  return h("section", { class: "panel" }, h("div", { class: "panel-header" },
    h("div", {}, h("h2", {}, "历史文件结构"), h("p", {}, node.id === state.selected.id ? "当前会话" : `${node.agent_nickname || "子代理"} · ${node.agent_path || node.title}`)),
    badge(`${num(node.segment_count)} 个文件`, node.index_verified ? "green" : "gray")),
    h("div", { class: "panel-body" },
      node.segments?.length > 1 ? h("p", { class: "segments-title" }, "这是同一会话的历史链。旧段保留回滚点之前的内容，后续记录接在新文件中。") : null,
      node.segments?.length ? h("div", { class: "segment-chain" }, node.segments.map((segment, index) => [index ? h("span", { class: "segment-arrow", "aria-hidden": "true" }, "→") : null,
        h("article", { class: `segment-card${index === node.segments.length - 1 ? " current" : ""}` },
          h("span", { class: "segment-label" }, index ? `历史段 ${index + 1}` : "初始记录"),
          h("h3", {}, index === node.segments.length - 1 ? "当前续写文件" : "保留的历史"),
          h("p", {}, segment.name.match(/\d{4}-\d{2}-\d{2}/)?.[0] || "本地文件"),
          h("p", {}, `${size(segment.bytes)} · ${num(segment.records)} 条记录`),
          segment.encrypted_blocks != null ? h("p", {}, `${num(segment.encrypted_blocks)} 个加密块`) : null)])) : h("div", { class: "before-inspect" }, node.reason || "暂时无法读取文件结构。"),
      node.index_verified ? h("div", { class: "check-strip" }, h("span", {}, "记录序号连续"), h("span", {}, "索引与文件对应"), h("span", {}, "可读历史保留")) : null,
      node.reason && !node.ready && node.encrypted_blocks !== 0 ? h("div", { class: "info-band" }, node.reason) : null,
      node.segments?.length ? h("details", { class: "technical" }, h("summary", {}, "查看文件位置与校验详情"),
        h("table", {}, h("tbody", {}, node.segments.map((segment, index) => h("tr", {}, h("th", {}, `第 ${index + 1} 段`), h("td", {},
          h("div", {}, segment.path), h("div", {}, `起始序号 ${num(segment.start_ordinal)} · ${num(segment.records)} 条记录`))))))) : null));
}
function renderThread() {
  const thread = state.selected, family = state.family;
  const nodes = state.inspection?.nodes || family.nodes;
  const root = nodes.find((n) => n.id === thread.id) || thread;
  const focused = nodes.find((n) => n.id === state.focused) || root;
  const selected = nodes.filter((n) => state.checked.has(n.id) && n.ready);
  const encrypted = selected.reduce((sum, n) => sum + n.encrypted_blocks, 0);
  const serverIds = selected.reduce((sum, n) => sum + (n.server_item_ids || 0), 0);
  const checkButton = button(state.inspection ? "重新检查" : "检查会话", startInspection, "primary");
  checkButton.id = "inspectButton";
  setContent(
    h("section", { class: "detail-heading" }, h("div", { class: "title-block" },
      family.parent ? button(`‹ 返回父会话 · ${family.parent.title.slice(0, 30)}`, () => selectThread(family.parent.id), "parent-link") : null,
      h("div", { class: "title-line" }, h("h1", {}, thread.title), badge(thread.is_subagent ? "子代理" : "主会话")),
      h("div", { class: "thread-id" }, thread.id, button("复制 ID", () => copy(thread.id), "copy-button")),
      h("div", { class: "thread-meta" }, h("span", { title: thread.cwd }, project(thread.cwd)), thread.model ? h("span", {}, thread.model) : null, h("span", {}, `更新于 ${date(thread.updated_at)}`))),
      h("div", { class: "inspection-controls" }, checkButton,
        h("label", { class: "check-label" }, h("input", { type: "checkbox", checked: state.includeChildren, disabled: state.busy || family.nodes.length < 2,
          onchange: (event) => { state.includeChildren = event.target.checked; } }), "一并检查子代理"))),
    h("div", { class: "stats" },
      stat("历史文件", state.inspection ? (root.segment_count || 0) : "待检查", state.inspection ? "段" : "", state.inspection ? (root.segment_count > 1 ? "回滚形成的历史引用链" : "独立会话文件") : "检查后展示完整结构"),
      stat("关联子代理", family.nodes.length - 1, "个", "包括下级子代理"),
      stat("当前会话加密块", state.inspection ? root.encrypted_blocks : "待检查", state.inspection && root.encrypted_blocks != null ? "个" : "", "存在加密内容不等于已损坏")),
    h("div", { class: "info-band" }, h("span", { class: "info-symbol", "aria-hidden": "true" }, "i"),
      h("span", {}, h("strong", {}, "只清理正在报错的会话。"), " 正常回复也会生成加密内容。本工具能检查结构和历史报错，无法离线判断某段密文是否失效。")),
    gatewayPanel(), treePanel(nodes), segmentPanel(focused),
    state.inspection ? h("section", { class: "selection-bar" },
      h("div", {}, h("strong", {}, `已选择 ${selected.length} 条会话${selected.some((n) => n.is_subagent) ? `，含 ${selected.filter((n) => n.is_subagent).length} 个子代理` : ""}`),
        h("p", {}, `将清理 ${num(encrypted)} 个加密块、${num(serverIds)} 个厂商条目引用，并同步历史引用和索引。`, h("br"), "开始前自动保存原文件与索引快照。")),
      h("div", { class: "actions-right" }, button("备份并清理", startClean, "primary", { id: "cleanButton", disabled: state.busy || !selected.length || !(encrypted + serverIds) }),
        h("small", {}, "完成后重新打开对应会话"))) : null);
}
async function refreshGateway(identity = state.selected?.id) {
  if (!identity) return;
  const info = await api(`/api/gateway/${encodeURIComponent(identity)}`);
  if (state.selected?.id === identity) {
    state.gateway = info; state.gatewayUpstream = info.upstream_url || "";
    state.gatewayProvider = info.provider_id || "custom";
    state.gatewayIncludeChildren = info.include_children ?? true; render();
  }
}
function gatewayPanel() {
  const gateway = state.gateway;
  if (!gateway) return null;
  const upstream = h("input", { id: "gatewayUpstream", type: "url", value: state.gatewayUpstream,
    placeholder: "https://你的厂商地址/v1", disabled: state.busy || gateway.enabled,
    oninput: (event) => { state.gatewayUpstream = event.target.value; } });
  const provider = h("input", { id: "gatewayProvider", value: state.gatewayProvider,
    disabled: state.busy || gateway.enabled, oninput: (event) => { state.gatewayProvider = event.target.value; } });
  return h("section", { class: "panel gateway-panel" },
    h("div", { class: "panel-header" }, h("div", {}, h("h2", {}, "当前会话代理"),
      h("p", {}, "先备份并清理，再过滤当前会话后续请求中的密文。可同时覆盖它的子代理。")),
      badge(gateway.enabled ? (gateway.connected ? "已接入" : "已开启，等待接入") : "未开启", gateway.enabled ? "green" : "gray")),
    h("div", { class: "panel-body" },
      h("div", { class: "gateway-fields" }, h("label", {}, "原厂商 API URL", upstream),
        h("label", {}, "Codex 厂商配置名称", provider)),
      h("label", { class: "check-label gateway-scope" }, h("input", { id: "gatewayIncludeChildren", type: "checkbox",
        checked: state.gatewayIncludeChildren, disabled: state.busy || gateway.enabled,
        onchange: (event) => { state.gatewayIncludeChildren = event.target.checked; } }),
        "一并处理子代理，包括后续新建的子代理"),
      gateway.proxy_url ? h("div", { class: "gateway-address" }, "此会话的代理 API URL", commandText(gateway.proxy_url)) : null,
      gateway.enabled ? h("div", { class: "gateway-detail" },
        h("p", {}, "退出原会话，用下面的命令重新打开，后续请求才会经过代理。保持本工具运行。"),
        commandText(gateway.resume_command),
        h("p", {}, `已匹配 ${num(gateway.filtered_requests)} 次请求，移除 ${num(gateway.removed)} 个加密块。`),
        gateway.last_error ? h("p", { class: "gateway-error" }, gateway.last_error) : null) :
        h("p", { class: "gateway-detail" }, "填写这条会话原来发送请求的 URL。开始前退出这条会话；开启后页面会给出接入命令。"),
      h("div", { class: "button-group" }, gateway.enabled ?
        button("关闭密文过滤", disableGateway, "secondary") : button("备份、清理并开启代理", enableGateway, "primary"),
        button("刷新代理状态", () => refreshGateway().catch((error) => notice(error.message)), "quiet"))));
}
function commandText(text) {
  return h("div", { class: "command-box" }, h("code", {}, text), button("复制", () => copy(text), "copy-button"));
}
function enableGateway() {
  const threadId = state.selected.id;
  const upstream = $("gatewayUpstream").value.trim(), provider = $("gatewayProvider").value.trim();
  runJob("/api/gateway/enable", { thread_id: threadId, upstream_url: upstream, provider_id: provider,
    include_children: state.gatewayIncludeChildren }, async () => {
    state.inspection = null; state.checked.clear();
    await refreshGateway(threadId);
    notice("当前会话已备份并清理。请用代理面板中的命令重新打开这条会话，接入后会显示请求统计。", true);
    await search();
  });
}
async function disableGateway() {
  try {
    await api("/api/gateway/disable", { thread_id: state.selected.id });
    await refreshGateway(); toast("已关闭这条会话的密文过滤，原厂商转发继续保留。");
  } catch (error) { notice(error.message); }
}
function startInspection() {
  runJob("/api/inspect", { thread_id: state.selected.id, include_children: state.includeChildren }, async (result) => {
    state.inspection = result; state.focused = state.selected.id;
    state.checked = new Set(result.nodes.filter((n) => n.ready && (n.id === state.selected.id || n.error_evidence?.latest_failed)).map((n) => n.id));
    toast("结构检查完成，请选择实际需要修复的会话。");
  });
}
function commandBox(identity) {
  const command = `codex resume ${identity}`;
  return h("div", { class: "command-box" }, h("code", {}, command), button("复制命令", () => copy(command), "copy-button", { disabled: false }));
}
function startClean() {
  const selected = [...state.checked];
  runJob("/api/clean", { inspection_id: state.inspection.inspection_id, thread_ids: selected }, async (result) => {
    for (const node of state.inspection.nodes) if (selected.includes(node.id)) { node.ready = false; node.encrypted_blocks = 0; node.server_item_ids = 0; node.cleaned = true; node.reason = "清理完成。"; }
    state.checked.clear();
    const identity = selected.includes(state.selected.id) ? state.selected.id : selected[0];
    notice(`清理完成。已处理 ${result.backup.roots.length} 条会话，备份和索引校验通过。\n退出原会话后重新打开，再发条消息确认服务端是否恢复。`, true,
      [commandBox(identity), h("div", { class: "button-group" }, button("查看本次修复记录", showBackups, "quiet small", { disabled: false }))]);
    await search();
  });
}

const backupStates = { applied: ["已清理", "green"], restored: ["已回滚", "blue"], recovered: ["已恢复清理前状态", "blue"],
  prepared: ["已备份，未完成", "amber"], applying: ["上次操作未完成", "amber"], restoring: ["恢复未完成", "amber"],
  needs_recovery: ["需要检查恢复", "amber"], failed: ["操作未完成", "gray"] };
function renderBackups() {
  setContent(h("div", { class: "backup-heading" }, h("div", {}, h("h1", {}, "修复记录"),
    h("p", {}, "每次清理都保存原文件与索引。已有新消息时，会阻止覆盖式回滚。")), button("刷新记录", showBackups, "secondary")),
    state.backups.length ? state.backups.map((item) => {
      const [label, color] = backupStates[item.state] || [item.state, "gray"];
      return h("article", { class: "backup-card" },
        h("div", { class: "backup-top" }, h("div", {}, h("div", { class: "backup-date" }, date(item.created_at || item.updated_at) || item.id),
          h("h2", { class: "backup-title" }, item.roots[0]?.title || "本地会话修复", item.roots.length > 1 ? ` 等 ${item.roots.length} 条会话` : "")), badge(label, color)),
        h("div", { class: "backup-meta" }, h("span", {}, `${item.roots.length} 条会话`), h("span", {}, `${item.segments} 段历史文件`), h("span", {}, `${num(item.encrypted_removed)} 个加密块`)),
        item.roots.length > 1 ? h("div", { class: "backup-roots" }, item.roots.map((root) => h("span", { title: root.id }, root.title))) : null,
        item.error ? h("p", { class: "backup-note" }, item.error) : null,
        item.can_recover ? h("p", { class: "backup-note" }, "这次操作没有完整结束。工具会先检查当前文件和索引，只恢复能够与备份对应的内容。") : null,
        h("div", { class: "backup-bottom" }, h("div", { class: "backup-path" }, "备份位置", h("br"), item.path),
          h("div", { class: "button-group" }, button("导出备份", () => exportBackup(item), "secondary"),
            item.can_restore ? button("回滚到清理前", () => restoreBackup(item, false), "danger-button") : null,
            item.can_recover ? button("恢复清理前状态", () => restoreBackup(item, true), "danger-button") : null)));
    }) : h("div", { class: "backups-empty" }, h("h2", {}, "这里会保存每一次修复"), h("p", {}, "完成首次清理后，可以在这里查看记录、导出备份或回滚。", h("br"), "也会识别兼容的本地历史修复备份。")));
}
function restoreBackup(item, recover) {
  runJob(`/api/backups/${encodeURIComponent(item.id)}/${recover ? "recover" : "restore"}`, {}, async () => {
    state.inspection = null; state.checked.clear();
    notice("已恢复清理前的文件与索引，校验通过。重新打开对应会话即可查看原记录。", true);
    await showBackups();
  });
}
async function exportBackup(item) {
  try {
    toast("正在打包备份…");
    const response = await fetch(`/api/backups/${encodeURIComponent(item.id)}/export`, { headers: { "X-Session-Token": state.token } });
    if (!response.ok) { const data = await response.json(); throw new Error(data.error); }
    const url = URL.createObjectURL(await response.blob());
    const anchor = h("a", { href: url, download: `${item.id}.zip` }); document.body.append(anchor); anchor.click(); anchor.remove();
    setTimeout(() => URL.revokeObjectURL(url), 3000); toast("备份已导出");
  } catch (error) { notice(error.message); }
}

$("searchInput").addEventListener("input", () => { clearTimeout(searchTimer); searchTimer = setTimeout(() => search(), 220); });
$("kindFilter").addEventListener("change", () => search()); $("archiveFilter").addEventListener("change", () => search());
$("loadMore").addEventListener("click", () => search(true));
$("sessionsTab").addEventListener("click", () => { state.tab = "sessions"; updateTabs(); render(); });
$("backupsTab").addEventListener("click", showBackups);
$("exitButton").addEventListener("click", async () => {
  try { await api("/api/shutdown", {}); document.body.replaceChildren(h("main", { class: "closed-message" }, h("h1", {}, "工具已退出"), h("p", {}, "可以关闭此页面。下次双击桌面入口即可重新打开。"))); }
  catch (error) { notice(error.message); }
});

async function boot() {
  renderWelcome();
  try {
    const info = await api("/api/bootstrap"); state.token = info.token;
    $("dataHome").textContent = info.home; $("dataHome").title = info.home;
    if (!info.home_exists) { notice("当前目录中没有找到 Codex 会话索引。请通过启动参数 --home 指定正确的数据目录。"); return; }
    await search();
    const identity = new URLSearchParams(location.search).get("thread");
    if (identity) await selectThread(identity);
  } catch (error) { notice(`无法连接本地服务：${error.message}`); }
}
boot();
