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
  const reducedMotionQuery = window.matchMedia ? window.matchMedia("(prefers-reduced-motion: reduce)") : null;
  const commandNodes = Array.from(document.querySelectorAll("[data-command-target]"));
  const commandMap = new Map();

  for (const node of commandNodes) {
    const target = node.getAttribute("data-command-target") || "";
    const label = node.getAttribute("data-command-label") || node.textContent || target;
    const kind = node.getAttribute("data-command-kind") || (target.startsWith("#") ? "panel" : "route");
    const queryValue = (node.getAttribute("data-command-query") || target).trim();
    const extraTerms = node.getAttribute("data-command-terms") || "";
    if (!target) {
      continue;
    }
    const key = `${kind}::${target}`;
    if (!commandMap.has(key)) {
      const panel = kind === "panel" && target.startsWith("#") ? document.querySelector(target) : null;
      const titleNode = panel instanceof HTMLElement ? panel.querySelector("h1, h2, h3") : null;
      const title = (titleNode && titleNode.textContent ? titleNode.textContent : label).trim();
      commandMap.set(key, {
        key,
        kind,
        target,
        label: label.trim(),
        queryValue,
        title,
        panel,
        elements: [],
        terms: `${label} ${queryValue} ${target} ${title} ${extraTerms} ${kind}`.toLowerCase(),
      });
    }
    commandMap.get(key).elements.push(node);
  }

  const commands = Array.from(commandMap.values());
  const panels = commands.filter((command) => command.kind === "panel");
  const panelMap = new Map(panels.map((panel) => [panel.target, panel]));
  const panelCount = panels.length;
  const globalCommandCount = commands.length - panelCount;
  let lastMatches = panels.length ? panels : commands;

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

  const readyMessage = () => {
    const panelLabel = `${panelCount} local panel${panelCount === 1 ? "" : "s"}`;
    const globalLabel = `${globalCommandCount} global command${globalCommandCount === 1 ? "" : "s"}`;
    return `Ready. ${panelLabel} and ${globalLabel} indexed.`;
  };

  const currentTarget = () => {
    const hash = window.location.hash || "#top";
    return panelMap.has(hash) ? hash : "#top";
  };

  const setStatus = (message) => {
    status.textContent = message;
  };

  const scrollBehavior = () => (reducedMotionQuery && reducedMotionQuery.matches ? "auto" : "smooth");

  const scrollPanelIntoView = (panel) => {
    panel?.scrollIntoView({ behavior: scrollBehavior(), block: "start" });
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
      setStatus(readyMessage());
    }
  };

  const matchedCommands = (query) => {
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      return panels.length ? panels : commands;
    }
    return commands.filter((command) => command.terms.includes(normalized));
  };

  const renderMatches = (matches, query) => {
    lastMatches = matches.length ? matches : (panels.length ? panels : commands);
    const normalized = normalizedCommandQuery(query);
    if (!normalized) {
      for (const command of commands) {
        for (const element of command.elements) {
          element.classList.remove("is-muted");
        }
      }
      highlightPanel(currentTarget());
      return;
    }

    for (const command of commands) {
      const isMatch = matches.includes(command);
      for (const element of command.elements) {
        element.classList.toggle("is-muted", !isMatch);
      }
    }

    if (matches.length) {
      setStatus(`${matches.length} command match${matches.length === 1 ? "" : "es"} for "${normalized}". Press Enter to jump.`);
    } else {
      setStatus(`No command matches for "${normalized}".`);
    }
  };

  const navigateTo = (command) => {
    if (!command) {
      return;
    }
    if (command.kind === "panel") {
      if (window.location.hash === command.target) {
        highlightPanel(command.target);
        scrollPanelIntoView(command.panel);
        return;
      }
      window.location.hash = command.target;
      return;
    }

    const absoluteUrl = new URL(command.target, window.location.href);
    if (
      absoluteUrl.origin === window.location.origin
      && absoluteUrl.pathname === window.location.pathname
      && absoluteUrl.hash
      && panelMap.has(absoluteUrl.hash)
    ) {
      if (window.location.hash === absoluteUrl.hash) {
        highlightPanel(absoluteUrl.hash);
        scrollPanelIntoView(panelMap.get(absoluteUrl.hash)?.panel);
      } else {
        window.location.hash = absoluteUrl.hash;
      }
      return;
    }

    window.location.href = absoluteUrl.toString();
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
    renderMatches(matchedCommands(input.value), input.value);
  });

  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      navigateTo(lastMatches[0] || panelMap.get(currentTarget()) || commands[0]);
      return;
    }
    if (event.key === "Escape") {
      input.blur();
      input.value = route;
      renderMatches(panels.length ? panels : commands, "");
    }
  });

  window.addEventListener("hashchange", () => {
    highlightPanel(currentTarget());
    renderMatches(matchedCommands(input.value), input.value);
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
    setStatus(`${readyMessage()} Last panel: ${panelMap.get(storedTarget).label}.`);
  } else {
    setStatus(readyMessage());
  }
  highlightPanel(initialTarget);
})();
