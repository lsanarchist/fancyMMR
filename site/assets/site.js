(() => {
  const surface = document.querySelector("[data-command-surface]");
  if (!surface) {
    return;
  }

  const input = surface.querySelector("[data-command-input]");
  const status = surface.querySelector("[data-command-status]");
  if (!(input instanceof HTMLInputElement) || !status) {
    return;
  }

  const route = surface.getAttribute("data-page-route") || window.location.pathname || "/";
  const storageKey = `fancymmr:last-panel:${route}`;
  const linkNodes = Array.from(document.querySelectorAll("[data-command-target]"));
  const panelMap = new Map();

  for (const node of linkNodes) {
    const target = node.getAttribute("data-command-target") || "";
    const label = node.getAttribute("data-command-label") || node.textContent || target;
    if (!target) {
      continue;
    }
    if (!panelMap.has(target)) {
      const panel = document.querySelector(target);
      const titleNode = panel ? panel.querySelector("h1, h2, h3") : null;
      const title = (titleNode && titleNode.textContent ? titleNode.textContent : label).trim();
      panelMap.set(target, {
        target,
        label: label.trim(),
        title,
        panel,
        elements: [],
        terms: `${label} ${target} ${title}`.toLowerCase(),
      });
    }
    panelMap.get(target).elements.push(node);
  }

  const panels = Array.from(panelMap.values());
  const panelCount = panels.length;
  let lastMatches = panels;

  const isEditableTarget = (eventTarget) => {
    if (!(eventTarget instanceof HTMLElement)) {
      return false;
    }
    return (
      eventTarget === input ||
      eventTarget.tagName === "INPUT" ||
      eventTarget.tagName === "TEXTAREA" ||
      eventTarget.tagName === "SELECT" ||
      eventTarget.isContentEditable
    );
  };

  const currentTarget = () => {
    const hash = window.location.hash || "#top";
    return panelMap.has(hash) ? hash : "#top";
  };

  const setStatus = (message) => {
    status.textContent = message;
  };

  const normalizedCommandQuery = (rawValue) => {
    const raw = rawValue.trim().toLowerCase();
    if (!raw) {
      return "";
    }
    const normalizedRoute = route.trim().toLowerCase();
    if (raw === normalizedRoute) {
      return "";
    }
    if (raw.startsWith(`${normalizedRoute} `)) {
      return raw.slice(normalizedRoute.length).trim();
    }
    return raw;
  };

  const syncInputValue = (target) => {
    if (document.activeElement === input) {
      return;
    }
    input.value = target === "#top" ? route : `${route} ${target}`;
  };

  const highlightPanel = (target) => {
    for (const panel of panels) {
      const isTarget = panel.target === target;
      for (const element of panel.elements) {
        element.classList.toggle("is-targeted", isTarget);
      }
      if (panel.panel) {
        panel.panel.classList.toggle("is-focused", isTarget);
      }
    }
    syncInputValue(target);
    const activePanel = panelMap.get(target);
    if (activePanel) {
      window.localStorage.setItem(storageKey, target);
      const index = panels.findIndex((panel) => panel.target === target);
      setStatus(`Focused ${activePanel.label}. Panel ${index + 1} of ${panelCount}.`);
    } else {
      setStatus(`Ready. ${panelCount} panels indexed for this route.`);
    }
  };

  const matchedPanels = (query) => {
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      return panels;
    }
    return panels.filter((panel) => panel.terms.includes(normalized));
  };

  const renderMatches = (matches, query) => {
    lastMatches = matches;
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      for (const panel of panels) {
        for (const element of panel.elements) {
          element.classList.remove("is-muted");
        }
      }
      highlightPanel(currentTarget());
      return;
    }

    for (const panel of panels) {
      const isMatch = matches.includes(panel);
      for (const element of panel.elements) {
        element.classList.toggle("is-muted", !isMatch);
      }
    }

    if (matches.length) {
      setStatus(`${matches.length} panel match${matches.length === 1 ? "" : "es"} for "${normalized}". Press Enter to jump.`);
    } else {
      setStatus(`No panel matches for "${normalized}".`);
    }
  };

  const navigateTo = (panel) => {
    if (!panel) {
      return;
    }
    if (window.location.hash === panel.target) {
      highlightPanel(panel.target);
      panel.panel?.scrollIntoView({ behavior: "smooth", block: "start" });
      return;
    }
    window.location.hash = panel.target;
  };

  const cyclePanels = (direction) => {
    if (!panelCount) {
      return;
    }
    const activeTarget = currentTarget();
    const currentIndex = Math.max(
      0,
      panels.findIndex((panel) => panel.target === activeTarget),
    );
    const nextIndex = (currentIndex + direction + panelCount) % panelCount;
    navigateTo(panels[nextIndex]);
  };

  input.addEventListener("input", () => {
    renderMatches(matchedPanels(input.value), input.value);
  });

  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      navigateTo(lastMatches[0] || panelMap.get(currentTarget()));
      return;
    }
    if (event.key === "Escape") {
      input.blur();
      input.value = "";
      renderMatches(panels, "");
    }
  });

  window.addEventListener("hashchange", () => {
    highlightPanel(currentTarget());
    renderMatches(matchedPanels(input.value), input.value);
  });

  document.addEventListener("keydown", (event) => {
    if ((event.key === "/" && !event.metaKey && !event.ctrlKey && !event.altKey) || ((event.key === "k" || event.key === "K") && event.ctrlKey)) {
      if (isEditableTarget(event.target)) {
        return;
      }
      event.preventDefault();
      input.focus();
      input.select();
      return;
    }

    if ((event.key === "[" || event.key === "]") && !isEditableTarget(event.target)) {
      event.preventDefault();
      cyclePanels(event.key === "]" ? 1 : -1);
    }
  });

  const initialTarget = currentTarget();
  const storedTarget = window.localStorage.getItem(storageKey);
  syncInputValue(initialTarget);
  if (!window.location.hash && storedTarget && panelMap.has(storedTarget)) {
    setStatus(`Ready. ${panelCount} panels indexed for this route. Last panel: ${panelMap.get(storedTarget).label}.`);
  } else {
    setStatus(`Ready. ${panelCount} panels indexed for this route.`);
  }
  highlightPanel(initialTarget);
})();
