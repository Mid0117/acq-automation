/* Atom Investments — shared chrome JS.
   Sourced from every shell page. Provides:
     - Theme management (light / dark, persisted, system-pref aware)
     - Profile dropdown
     - Modal primitives (open/close/focus-trap/ESC)
     - Quarter progression helpers
     - Project add modal (with workflow_dispatch hook)
     - Member add modal
*/

(function () {
  "use strict";

  // ---------------- Theme ------------------------------------------------
  const THEME_KEY = "atom.theme.v1";

  function getStoredTheme() {
    try { return localStorage.getItem(THEME_KEY); } catch { return null; }
  }
  function setStoredTheme(v) {
    try { localStorage.setItem(THEME_KEY, v); } catch {}
  }
  function applyTheme(theme) {
    document.documentElement.setAttribute("data-theme", theme);
    document.querySelectorAll('meta[name="theme-color"]').forEach(m => {
      m.setAttribute("content", theme === "dark" ? "#080C16" : "#0A1F44");
    });
    document.dispatchEvent(new CustomEvent("atom:theme", { detail: { theme } }));
  }
  function initTheme() {
    const stored = getStoredTheme();
    if (stored === "dark" || stored === "light") {
      applyTheme(stored);
      return;
    }
    const prefersDark = window.matchMedia &&
      window.matchMedia("(prefers-color-scheme: dark)").matches;
    applyTheme(prefersDark ? "dark" : "light");
  }
  function toggleTheme() {
    const cur = document.documentElement.getAttribute("data-theme") || "light";
    const next = cur === "dark" ? "light" : "dark";
    setStoredTheme(next);
    applyTheme(next);
  }
  // Apply BEFORE first paint so we don't flash white on dark.
  initTheme();

  window.Atom = window.Atom || {};
  Atom.toggleTheme = toggleTheme;
  Atom.getTheme = () => document.documentElement.getAttribute("data-theme") || "light";

  // ---------------- Modal primitive --------------------------------------
  let activeModal = null;
  let lastFocused = null;

  function trapFocus(modal, e) {
    if (e.key !== "Tab") return;
    const f = modal.querySelectorAll(
      'a[href], button:not([disabled]), input:not([disabled]), textarea:not([disabled]), select:not([disabled]), [tabindex]:not([tabindex="-1"])'
    );
    if (!f.length) return;
    const first = f[0], last = f[f.length - 1];
    if (e.shiftKey && document.activeElement === first) {
      last.focus(); e.preventDefault();
    } else if (!e.shiftKey && document.activeElement === last) {
      first.focus(); e.preventDefault();
    }
  }

  function openModal(html, opts) {
    opts = opts || {};
    closeModal(); // single-modal policy
    lastFocused = document.activeElement;

    const back = document.createElement("div");
    back.className = "modal-backdrop";
    back.innerHTML = html;
    document.body.appendChild(back);
    // Force reflow then add is-open so the transition runs.
    void back.offsetWidth;
    back.classList.add("is-open");
    activeModal = back;

    // Wire close affordances
    back.addEventListener("click", (e) => {
      if (e.target === back && opts.dismissOnBackdrop !== false) closeModal();
    });
    back.querySelectorAll("[data-modal-close]").forEach(el => {
      el.addEventListener("click", (e) => { e.preventDefault(); closeModal(); });
    });
    const escHandler = (e) => {
      if (e.key === "Escape") { e.preventDefault(); closeModal(); }
      else trapFocus(back, e);
    };
    back._escHandler = escHandler;
    document.addEventListener("keydown", escHandler);

    // Focus the first focusable inside
    setTimeout(() => {
      const first = back.querySelector(
        'input, textarea, select, button:not([data-modal-close])'
      );
      if (first) first.focus();
    }, 30);

    if (typeof opts.onOpen === "function") opts.onOpen(back);
    return back;
  }

  function closeModal() {
    if (!activeModal) return;
    const m = activeModal;
    activeModal = null;
    if (m._escHandler) document.removeEventListener("keydown", m._escHandler);
    m.classList.remove("is-open");
    setTimeout(() => { try { m.remove(); } catch {} }, 200);
    if (lastFocused && typeof lastFocused.focus === "function") {
      try { lastFocused.focus(); } catch {}
    }
  }

  Atom.openModal = openModal;
  Atom.closeModal = closeModal;

  // ---------------- Profile dropdown -------------------------------------
  // Mount-once. Reads the current "user" from a globally-set Atom.user, or
  // from a window-level seed. Until Supabase is wired, this is sourced from
  // the static roster — atom.js doesn't make network calls.
  function getUser() {
    return Atom.user || {
      id: "mido",
      name: "Mido Yasser",
      email: "mido@atompropertygroup.org",
      initials: "MY",
      role: "Operator",
    };
  }

  function mountProfileDropdown() {
    const trigger = document.querySelector("[data-profile-trigger]");
    if (!trigger) return;
    trigger.addEventListener("click", (e) => {
      e.preventDefault(); e.stopPropagation();
      const existing = document.getElementById("atom-profile-dd");
      if (existing) { existing.remove(); return; }
      const u = getUser();
      const dd = document.createElement("div");
      dd.id = "atom-profile-dd";
      dd.className = "profile-dd";
      const dark = Atom.getTheme() === "dark";
      dd.innerHTML = `
        <div class="profile-dd-head">
          <span class="avatar lg ${u.id}">${u.initials}</span>
          <div>
            <div class="name" contenteditable="false">${u.name}</div>
            <div class="email">${u.email}</div>
          </div>
        </div>
        <div class="dd-row" data-action="settings">
          <span class="ic">⚙</span><span>Settings</span>
        </div>
        <div class="dd-row" data-action="switch">
          <span class="ic">⇄</span><span>Switch project</span>
        </div>
        <div class="dd-row" data-action="theme">
          <span class="ic">◐</span><span>Dark mode</span>
          <span class="dd-toggle"><span class="switch ${dark ? "is-on" : ""}"></span></span>
        </div>
        <div class="dd-sep"></div>
        <div class="dd-row" data-action="logout">
          <span class="ic">⎋</span><span>Log out</span>
        </div>
      `;
      document.body.appendChild(dd);
      // Position next to the trigger
      const r = trigger.getBoundingClientRect();
      dd.style.right = (window.innerWidth - r.right) + "px";
      dd.style.top = (r.bottom + 8) + "px";
      requestAnimationFrame(() => dd.classList.add("is-open"));

      // Wire rows
      dd.querySelectorAll(".dd-row").forEach(row => {
        row.addEventListener("click", (ev) => {
          const a = row.getAttribute("data-action");
          if (a === "theme") {
            Atom.toggleTheme();
            const sw = row.querySelector(".switch");
            if (sw) sw.classList.toggle("is-on");
            ev.stopPropagation();
            return;
          }
          if (a === "logout") {
            // Worker handles /logout; on static gh-pages the guard already
            // redirects to Worker login.
            location.href = "https://apg-dashboard.mithchell.workers.dev/logout";
            return;
          }
          if (a === "settings" || a === "switch") {
            alert(a === "settings"
              ? "Settings is a Supabase-backed surface — shipping next session."
              : "Project switcher arrives with the Projects tab refresh — coming up.");
          }
          dd.remove();
        });
      });

      // Click-away
      setTimeout(() => {
        document.addEventListener("click", function close(ev) {
          if (!dd.contains(ev.target)) {
            dd.classList.remove("is-open");
            setTimeout(() => { try { dd.remove(); } catch {} }, 160);
            document.removeEventListener("click", close);
          }
        });
      }, 50);
    });
  }

  // ---------------- Quarter progression ----------------------------------
  /**
   * Compute % of tasks completed within a quarter window.
   * @param {Array<{status:string,end?:string,start?:string}>} tasks
   * @param {string} quarterKey  e.g. "2026-Q2"
   * @returns {{total:number, done:number, inflight:number, blocked:number, pct:number}}
   */
  function quarterProgress(tasks, quarterKey) {
    const [yr, qn] = quarterKey.split("-Q").map(Number);
    const startM = (qn - 1) * 3;
    const inQ = (tasks || []).filter(t => {
      const ref = t.end || t.start;
      if (!ref) return false;
      const d = new Date(ref);
      return d.getFullYear() === yr && Math.floor(d.getMonth() / 3) === qn - 1;
    });
    const done = inQ.filter(t => t.status === "done" || t.status === "shipped").length;
    const inflight = inQ.filter(t => t.status === "inflight").length;
    const blocked = inQ.filter(t => t.status === "blocked").length;
    return {
      total: inQ.length, done, inflight, blocked,
      pct: inQ.length ? Math.round((done / inQ.length) * 100) : 0,
      tasks: inQ,
    };
  }

  /**
   * Determine the "current" quarter relative to today.
   */
  function currentQuarterKey(now) {
    const d = now || new Date();
    return `${d.getFullYear()}-Q${Math.floor(d.getMonth() / 3) + 1}`;
  }

  /**
   * Find the top blocker for a project — first blocked task by end date.
   */
  function topBlocker(tasks) {
    const blocked = (tasks || []).filter(t => t.status === "blocked");
    if (!blocked.length) return null;
    blocked.sort((a, b) => (a.end || "9999").localeCompare(b.end || "9999"));
    return blocked[0];
  }

  /**
   * Render the SVG ring used on project cards + detail pages.
   * Caller wraps with a container that sets --accent.
   */
  function renderQuarterRing(pct, opts) {
    opts = opts || {};
    const size = opts.size || 44;
    const stroke = opts.stroke || 4;
    const r = (size - stroke) / 2;
    const c = 2 * Math.PI * r;
    const off = c * (1 - Math.max(0, Math.min(100, pct)) / 100);
    return `
      <div class="q-ring" style="width:${size}px; height:${size}px;">
        <svg viewBox="0 0 ${size} ${size}">
          <circle class="q-ring-track" cx="${size/2}" cy="${size/2}" r="${r}"
                  fill="none" stroke-width="${stroke}"></circle>
          <circle class="q-ring-fill"  cx="${size/2}" cy="${size/2}" r="${r}"
                  fill="none" stroke-width="${stroke}" stroke-linecap="round"
                  stroke-dasharray="${c.toFixed(2)}" stroke-dashoffset="${off.toFixed(2)}"></circle>
        </svg>
        <span class="q-ring-pct">${pct}%</span>
      </div>`;
  }

  Atom.quarterProgress = quarterProgress;
  Atom.currentQuarterKey = currentQuarterKey;
  Atom.topBlocker = topBlocker;
  Atom.renderQuarterRing = renderQuarterRing;

  // ---------------- Add Project modal ------------------------------------
  Atom.openAddProjectModal = function (opts) {
    opts = opts || {};
    const roster = opts.roster || [];
    const colors = [
      { name: "Gold",   hex: "#F5C518" },
      { name: "Violet", hex: "#7C5CD1" },
      { name: "Green",  hex: "#16A66B" },
      { name: "Red",    hex: "#D93B3B" },
      { name: "Sky",    hex: "#2D8DD9" },
      { name: "Slate",  hex: "#34406B" },
    ];
    const members = roster.map(m =>
      `<label><input type="checkbox" name="members" value="${m.id}" ${m.id === "mido" ? "checked" : ""}>
       <span class="avatar ${m.id}" style="width:18px;height:18px;font-size:8px">${m.initials}</span> ${m.name}</label>`
    ).join("");

    const html = `
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="add-proj-title">
        <div class="modal-head">
          <h3 class="modal-title" id="add-proj-title">New project</h3>
          <p class="modal-sub">Add a new initiative under the Atom Investments umbrella.</p>
        </div>
        <div class="modal-body">
          <div class="field">
            <label for="ap-name">Name</label>
            <input id="ap-name" name="name" type="text" placeholder="e.g. Kin v2" autocomplete="off">
          </div>
          <div class="field">
            <label>Accent color</label>
            <div class="swatches" id="ap-swatches">
              ${colors.map((c, i) => `<div class="swatch ${i === 0 ? "is-selected" : ""}" data-color="${c.hex}" title="${c.name}" style="background:${c.hex}"></div>`).join("")}
            </div>
          </div>
          <div class="field">
            <label for="ap-desc">Description</label>
            <textarea id="ap-desc" name="description" rows="2" placeholder="One sentence — what is this project?"></textarea>
          </div>
          <div class="field">
            <label>Initial members</label>
            <div class="member-checks">${members || "<em>No roster loaded.</em>"}</div>
          </div>
        </div>
        <div class="modal-foot">
          <button class="btn" data-modal-close>Cancel</button>
          <button class="btn btn-primary" id="ap-create">Create project</button>
        </div>
      </div>`;

    const back = Atom.openModal(html);
    let pickedColor = colors[0].hex;
    back.querySelectorAll(".swatch").forEach(sw => {
      sw.addEventListener("click", () => {
        back.querySelectorAll(".swatch").forEach(s => s.classList.remove("is-selected"));
        sw.classList.add("is-selected");
        pickedColor = sw.getAttribute("data-color");
      });
    });

    back.querySelector("#ap-create").addEventListener("click", async () => {
      const name = back.querySelector("#ap-name").value.trim();
      const description = back.querySelector("#ap-desc").value.trim();
      const memberIds = Array.from(back.querySelectorAll('input[name="members"]:checked')).map(i => i.value);
      if (!name) {
        back.querySelector("#ap-name").focus();
        return;
      }
      const payload = { name, color: pickedColor, description, members: memberIds };
      if (typeof opts.onCreate === "function") {
        try {
          await opts.onCreate(payload);
          Atom.closeModal();
        } catch (e) {
          alert("Failed to create: " + e.message);
        }
      } else {
        // Fallback: stub call to the workflow_dispatch endpoint placeholder.
        // The actual `add_project.yml` endpoint is built in next session.
        console.warn("[atom] add_project dispatch stub", payload);
        alert("Project drafted locally — workflow_dispatch endpoint `add_project.yml` ships next session.\n\nPayload:\n" + JSON.stringify(payload, null, 2));
        Atom.closeModal();
      }
    });
  };

  // ---------------- Add Member modal -------------------------------------
  Atom.openAddMemberModal = function (opts) {
    opts = opts || {};
    const projectId = opts.projectId || "";
    const html = `
      <div class="modal" role="dialog" aria-modal="true" aria-labelledby="add-mem-title">
        <div class="modal-head">
          <h3 class="modal-title" id="add-mem-title">Invite teammate</h3>
          <p class="modal-sub">${projectId ? "Add to <strong>" + projectId.toUpperCase() + "</strong>." : "Send a magic-link invite."}</p>
        </div>
        <div class="modal-body">
          <div class="field">
            <label for="am-name">Full name</label>
            <input id="am-name" type="text" autocomplete="off" placeholder="e.g. Sam Operator">
          </div>
          <div class="field">
            <label for="am-email">Email</label>
            <input id="am-email" type="email" autocomplete="off" placeholder="sam@atompropertygroup.org">
          </div>
          <div class="field">
            <label for="am-role">Role</label>
            <select id="am-role">
              <option value="operator">Operator</option>
              <option value="manager">Manager</option>
              <option value="viewer">Viewer</option>
            </select>
          </div>
        </div>
        <div class="modal-foot">
          <button class="btn" data-modal-close>Cancel</button>
          <button class="btn btn-primary" id="am-invite">Send invite</button>
        </div>
      </div>`;
    const back = Atom.openModal(html);
    back.querySelector("#am-invite").addEventListener("click", async () => {
      const name = back.querySelector("#am-name").value.trim();
      const email = back.querySelector("#am-email").value.trim();
      const role = back.querySelector("#am-role").value;
      if (!name || !email) return;
      const payload = { name, email, role, projectId };
      if (typeof opts.onInvite === "function") {
        try { await opts.onInvite(payload); Atom.closeModal(); }
        catch (e) { alert("Failed: " + e.message); }
      } else {
        console.warn("[atom] add_member dispatch stub", payload);
        alert("Invite drafted — Supabase auth + `add_member.yml` workflow ship next session.\n\n" + JSON.stringify(payload, null, 2));
        Atom.closeModal();
      }
    });
  };

  // ---------------- Bootstrap on DOMContentLoaded ------------------------
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", mountProfileDropdown);
  } else {
    mountProfileDropdown();
  }
})();
