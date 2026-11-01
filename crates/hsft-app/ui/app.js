"use strict";

const { invoke } = window.__TAURI__.core;
const { listen } = window.__TAURI__.event;
const { open } = window.__TAURI__.dialog;
const { getCurrentWebview } = window.__TAURI__.webview;

const list = document.getElementById("friends");
const hint = document.getElementById("hint");
const add = document.getElementById("add");
const settings = document.getElementById("settings");

let friends = [];
const transfers = new Map();

function size(bytes) {
  const units = ["B", "KB", "MB", "GB", "TB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? value : value.toFixed(1)} ${units[unit]}`;
}

function el(tag, className, text) {
  const node = document.createElement(tag);
  if (className) node.className = className;
  if (text !== undefined) node.textContent = text;
  return node;
}

function button(label, style, action) {
  const node = el("button", style, label);
  node.addEventListener("click", action);
  return node;
}

function showError(id, message) {
  const node = document.getElementById(id);
  node.textContent = message || "";
  node.hidden = !message;
}

function friendName(id) {
  return friends.find((friend) => friend.id === id)?.name ?? "A friend";
}

function transfer(id) {
  if (!transfers.has(id)) {
    transfers.set(id, { id, state: "running", done: 0, bytes: 0, speed: 0, title: "Preparing" });
  }
  return transfers.get(id);
}

function detailText(t) {
  switch (t.state) {
    case "offer":
      return `Wants to send ${t.title}: ${size(t.bytes)}, ${t.files} ${t.files === 1 ? "file" : "files"}`;
    case "done":
      return t.direction === "receive" ? `Saved ${t.title}` : `Delivered ${t.title}`;
    case "failed":
      return t.error;
    default: {
      const route = { direct: "direct", relay: "through a relay" }[t.route];
      const verb = t.direction === "receive" ? "Receiving" : "Sending";
      const speed = t.speed ? `, ${size(t.speed)}/s` : "";
      return t.route
        ? `${verb} ${t.title}: ${size(t.done)} of ${size(t.bytes)}${speed}${route ? `, ${route}` : ""}`
        : `Connecting to send ${t.title}`;
    }
  }
}

function progressPercent(t) {
  if (t.state === "done") return 100;
  if (t.state !== "running" || !t.bytes) return 0;
  return Math.min(100, (t.done / t.bytes) * 100);
}

function transferActions(t) {
  const clear = () => {
    transfers.delete(t.id);
    render();
  };
  switch (t.state) {
    case "offer":
      return [
        button("Accept", "primary", () => invoke("respond", { transfer: t.id, accept: true })),
        button("Decline", "quiet", () => {
          invoke("respond", { transfer: t.id, accept: false });
          clear();
        }),
      ];
    case "running":
      return [button("Stop", "quiet", () => invoke("cancel", { transfer: t.id }))];
    default:
      return [
        ...(t.savedTo
          ? [button("Show in folder", "quiet", () => invoke("reveal", { path: t.savedTo }))]
          : []),
        button("Dismiss", "quiet", clear),
      ];
  }
}

function pick(friend, directory) {
  return async () => {
    const chosen = await open({ multiple: !directory, directory });
    if (chosen) send(friend.id, [chosen].flat());
  };
}

async function send(friendId, paths) {
  try {
    const id = await invoke("send_files", { friend: friendId, paths });
    transfer(id).friendId = friendId;
  } catch (error) {
    const id = `local-${Date.now()}`;
    Object.assign(transfer(id), { friendId, state: "failed", error: String(error) });
  }
  render();
}

function buildRow(name, friendId, t, thin) {
  const row = el("div", "row");
  row.classList.toggle("thin", thin);
  if (friendId) row.dataset.friend = friendId;

  const detail = t ? detailText(t) : "";
  const fill = (layer) => {
    layer.append(el("div", "name", name));
    if (t) layer.append(el("div", "detail", detail));
    return layer;
  };
  row.append(fill(el("div", "layer")));

  if (t) {
    row.dataset.transfer = t.id;
    row.classList.add("wrap");
    row.classList.toggle("failed", t.state === "failed");
    row.style.setProperty("--done", `${progressPercent(t)}%`);
    const flood = fill(el("div", "layer flood"));
    flood.setAttribute("aria-hidden", "true");
    row.append(flood);
  } else {
    row.classList.add("rest");
  }

  const actions = el("div", "actions");
  if (t) {
    actions.append(...transferActions(t));
  } else {
    const friend = friends.find((f) => f.id === friendId);
    actions.append(
      button("Send files", "quiet", pick(friend, false)),
      button("Folder", "quiet", pick(friend, true)),
    );
  }
  row.append(actions);
  return row;
}

function render() {
  list.replaceChildren();
  hint.hidden = friends.length === 0;
  if (friends.length === 0) {
    const empty = el("div", "empty");
    empty.append(
      el("h2", "", "Add your first friend"),
      el("p", "", "Files go straight from your PC to theirs. Friends connect once with an invite code."),
      button("Add friend", "primary", openAdd),
    );
    list.append(empty);
    return;
  }
  for (const friend of friends) {
    const theirs = [...transfers.values()].filter((t) => t.friendId === friend.id);
    list.append(buildRow(friend.name, friend.id, null, false));
    const [first, ...rest] = theirs;
    if (first) {
      list.lastChild.replaceWith(buildRow(friend.name, friend.id, first, false));
      for (const t of rest) list.append(buildRow(t.title, friend.id, t, true));
    }
  }
  const strays = [...transfers.values()].filter(
    (t) => t.friendId && !friends.some((friend) => friend.id === t.friendId),
  );
  for (const t of strays) list.append(buildRow(t.title, t.friendId, t, true));
}

/** Updates one row in place, so buttons keep working while progress ticks. */
function updateRow(t) {
  const row = list.querySelector(`[data-transfer="${t.id}"]`);
  if (!row) return render();
  row.style.setProperty("--done", `${progressPercent(t)}%`);
  const text = detailText(t);
  for (const detail of row.querySelectorAll(".detail")) detail.textContent = text;
}

async function refresh() {
  const state = await invoke("overview");
  friends = state.friends;
  document.getElementById("name").value = state.name;
  document.getElementById("folder").textContent = state.downloadDir;
  render();
}

listen("hsft", ({ payload: event }) => {
  switch (event.type) {
    case "friendAdded":
      refresh();
      add.close();
      break;
    case "offer":
      Object.assign(transfer(event.transfer), {
        state: "offer",
        friendId: event.fromId,
        title: event.title,
        files: event.files,
        bytes: event.bytes,
      });
      render();
      break;
    case "started":
      Object.assign(transfer(event.transfer), {
        state: "running",
        friendId: event.peerId,
        direction: event.direction,
        title: event.title,
        bytes: event.bytes,
      });
      render();
      break;
    case "progress": {
      const t = transfer(event.transfer);
      Object.assign(t, {
        done: event.done,
        bytes: event.total,
        speed: event.bytesPerSec,
        route: event.route,
      });
      updateRow(t);
      break;
    }
    case "finished":
      // A declined offer was already cleared when the button was pressed.
      if (transfers.has(event.transfer)) {
        Object.assign(transfer(event.transfer), { state: "done", savedTo: event.savedTo });
        render();
      }
      break;
    case "failed":
      Object.assign(transfer(event.transfer), { state: "failed", error: event.error, speed: 0 });
      render();
      break;
  }
});

/* Dropping files on a friend. Positions arrive in physical pixels. */
let hovered = null;

function rowAt(position) {
  const scale = window.devicePixelRatio || 1;
  return document
    .elementFromPoint(position.x / scale, position.y / scale)
    ?.closest("[data-friend]");
}

function hover(row) {
  hovered?.classList.remove("over");
  hovered = row ?? null;
  hovered?.classList.add("over");
}

getCurrentWebview().onDragDropEvent(({ payload }) => {
  if (payload.type === "enter" || payload.type === "over") {
    hover(rowAt(payload.position));
  } else if (payload.type === "drop") {
    const row = rowAt(payload.position);
    hover(null);
    if (row && payload.paths.length) send(row.dataset.friend, payload.paths);
  } else {
    hover(null);
  }
});

/* Adding friends */

function openAdd() {
  showError("add-error");
  document.getElementById("invite-result").hidden = true;
  document.getElementById("join-code").value = "";
  add.showModal();
}

document.getElementById("open-add").addEventListener("click", openAdd);

document.getElementById("make-invite").addEventListener("click", async (event) => {
  showError("add-error");
  event.target.disabled = true;
  try {
    document.getElementById("invite-code").value = await invoke("create_invite");
    document.getElementById("invite-result").hidden = false;
  } catch (error) {
    showError("add-error", String(error));
  }
  event.target.disabled = false;
});

document.getElementById("copy-invite").addEventListener("click", async (event) => {
  await navigator.clipboard.writeText(document.getElementById("invite-code").value);
  event.target.textContent = "Copied";
  setTimeout(() => (event.target.textContent = "Copy code"), 1500);
});

document.getElementById("join").addEventListener("click", async (event) => {
  showError("add-error");
  event.target.disabled = true;
  try {
    await invoke("join_friend", { code: document.getElementById("join-code").value });
    await refresh();
    add.close();
  } catch (error) {
    showError("add-error", String(error));
  }
  event.target.disabled = false;
});

/* Settings */

function renderFriendAdmin() {
  const admin = document.getElementById("friend-admin");
  admin.replaceChildren();
  if (friends.length === 0) admin.append(el("li", "", "No friends yet."));
  for (const friend of friends) {
    const item = el("li");
    const remove = button("Remove", "quiet", async () => {
      if (remove.textContent === "Remove") {
        remove.textContent = "Remove for good?";
        return;
      }
      await invoke("remove_friend", { id: friend.id });
      await refresh();
      renderFriendAdmin();
    });
    item.append(el("span", "", friend.name), remove);
    admin.append(item);
  }
}

document.getElementById("open-settings").addEventListener("click", () => {
  showError("settings-error");
  renderFriendAdmin();
  settings.showModal();
});

document.getElementById("name").addEventListener("change", async (event) => {
  try {
    await invoke("set_name", { name: event.target.value });
    showError("settings-error");
  } catch (error) {
    showError("settings-error", String(error));
  }
});

document.getElementById("change-folder").addEventListener("click", async () => {
  const path = await open({ directory: true });
  if (path) {
    await invoke("set_download_dir", { path });
    await refresh();
  }
});

refresh();
