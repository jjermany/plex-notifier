/**
 * Plex Notifier - Modern UI JavaScript
 * Handles interactivity, form validation, and UX enhancements
 */

// ========== Utility Functions ==========

/**
 * Toggle password visibility for input fields
 * @param {string} inputId - ID of the input element
 * @param {string} iconId - ID of the icon element
 */
function togglePasswordVisibility(inputId, iconId) {
  const input = document.getElementById(inputId);
  const icon = document.getElementById(iconId);

  if (!input || !icon) return;

  if (input.type === 'password') {
    input.type = 'text';
    icon.classList.replace('bi-eye', 'bi-eye-slash');
  } else {
    input.type = 'password';
    icon.classList.replace('bi-eye-slash', 'bi-eye');
  }
}

/**
 * Disable button and show loading state
 * @param {HTMLButtonElement} button - Button element to disable
 */
function setButtonLoading(button) {
  if (!button) return;

  button.setAttribute('disabled', 'disabled');
  button.classList.add('loading');

  // Store original text
  if (!button.dataset.originalText) {
    button.dataset.originalText = button.textContent;
  }
}

/**
 * Re-enable button and remove loading state
 * @param {HTMLButtonElement} button - Button element to enable
 */
function removeButtonLoading(button) {
  if (!button) return;

  button.removeAttribute('disabled');
  button.classList.remove('loading');

  // Restore original text if available
  if (button.dataset.originalText) {
    button.textContent = button.dataset.originalText;
  }
}

/**
 * Validate email format
 * @param {string} email - Email address to validate
 * @returns {boolean} True if valid email format
 */
function isValidEmail(email) {
  const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
  return emailRegex.test(email);
}

/**
 * Validate URL format
 * @param {string} url - URL to validate
 * @returns {boolean} True if valid URL format
 */
function isValidUrl(url) {
  try {
    new URL(url);
    return true;
  } catch {
    return false;
  }
}

/**
 * Show validation error on form field
 * @param {HTMLElement} field - Form field element
 * @param {string} message - Error message to display
 */
function showFieldError(field, message) {
  if (!field) return;

  field.classList.add('is-invalid');

  // Remove existing error message if present
  const existingError = field.parentElement.querySelector('.invalid-feedback');
  if (existingError) {
    existingError.remove();
  }

  // Create new error message
  const errorDiv = document.createElement('div');
  errorDiv.className = 'invalid-feedback d-block';
  errorDiv.textContent = message;
  field.parentElement.appendChild(errorDiv);
}

/**
 * Clear validation error from form field
 * @param {HTMLElement} field - Form field element
 */
function clearFieldError(field) {
  if (!field) return;

  field.classList.remove('is-invalid');

  const errorDiv = field.parentElement.querySelector('.invalid-feedback');
  if (errorDiv) {
    errorDiv.remove();
  }
}

/**
 * Show toast notification
 * @param {string} message - Message to display
 * @param {string} type - Type of notification (success, error, info)
 */
function showToast(message, type = 'info') {
  // Create toast container if it doesn't exist
  let toastContainer = document.getElementById('toast-container');
  if (!toastContainer) {
    toastContainer = document.createElement('div');
    toastContainer.id = 'toast-container';
    toastContainer.style.cssText = `
      position: fixed;
      top: 20px;
      right: 20px;
      z-index: 9999;
    `;
    document.body.appendChild(toastContainer);
  }

  // Create toast element
  const toast = document.createElement('div');
  toast.className = `alert alert-${type} alert-dismissible fade show`;
  toast.style.cssText = `
    min-width: 250px;
    margin-bottom: 10px;
    box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
  `;
  toast.innerHTML = `
    ${message}
    <button type="button" class="btn-close" data-bs-dismiss="alert" aria-label="Close"></button>
  `;

  toastContainer.appendChild(toast);

  // Auto-remove after 5 seconds
  setTimeout(() => {
    toast.classList.remove('show');
    setTimeout(() => toast.remove(), 300);
  }, 5000);
}

// ========== Settings Page Functions ==========

/**
 * Initialize settings page functionality
 */
function initSettingsPage() {
  // Password toggle for Plex token
  const togglePlexTokenBtn = document.getElementById('togglePlexToken');
  if (togglePlexTokenBtn) {
    togglePlexTokenBtn.addEventListener('click', () => {
      togglePasswordVisibility('plex_token', 'iconPlexToken');
    });
  }

  // Password toggle for Tautulli API key
  const toggleTautulliKeyBtn = document.getElementById('toggleTautulliKey');
  if (toggleTautulliKeyBtn) {
    toggleTautulliKeyBtn.addEventListener('click', () => {
      togglePasswordVisibility('tautulli_api_key', 'iconTautulliKey');
    });
  }

  // Settings form submission
  const settingsForm = document.querySelector('form[action*="settings"]');
  if (settingsForm) {
    settingsForm.addEventListener('submit', function(e) {
      // Clear previous errors
      const fields = settingsForm.querySelectorAll('.form-control');
      fields.forEach(field => clearFieldError(field));

      let isValid = true;

      // Validate URLs
      const plexUrl = document.getElementById('plex_url');
      if (plexUrl && plexUrl.value && !isValidUrl(plexUrl.value)) {
        showFieldError(plexUrl, 'Please enter a valid URL (e.g., http://localhost:32400)');
        isValid = false;
      }

      const tautulliUrl = document.getElementById('tautulli_url');
      if (tautulliUrl && tautulliUrl.value && !isValidUrl(tautulliUrl.value)) {
        showFieldError(tautulliUrl, 'Please enter a valid URL (e.g., http://localhost:8181)');
        isValid = false;
      }

      const baseUrl = document.getElementById('base_url');
      if (baseUrl && baseUrl.value && !isValidUrl(baseUrl.value)) {
        showFieldError(baseUrl, 'Please enter a valid URL (e.g., http://localhost:5000)');
        isValid = false;
      }

      // Validate email
      const fromAddress = document.getElementById('from_address');
      if (fromAddress && fromAddress.value && !isValidEmail(fromAddress.value)) {
        showFieldError(fromAddress, 'Please enter a valid email address');
        isValid = false;
      }

      // Validate SMTP port
      const smtpPort = document.getElementById('smtp_port');
      if (smtpPort && smtpPort.value) {
        const port = parseInt(smtpPort.value);
        if (isNaN(port) || port < 1 || port > 65535) {
          showFieldError(smtpPort, 'Port must be between 1 and 65535');
          isValid = false;
        }
      }

      // Validate notify interval
      const notifyInterval = document.getElementById('notify_interval');
      if (notifyInterval && notifyInterval.value) {
        const interval = parseInt(notifyInterval.value);
        if (isNaN(interval) || interval < 1) {
          showFieldError(notifyInterval, 'Interval must be at least 1 minute');
          isValid = false;
        }
      }

      if (!isValid) {
        e.preventDefault();
        showToast('Please fix the errors in the form', 'danger');
        return false;
      }

      // Disable submit button
      const submitBtn = settingsForm.querySelector('button[type="submit"]');
      setButtonLoading(submitBtn);
    });
  }

  // Run check form submission
  const runForm = document.getElementById('runForm');
  if (runForm) {
    runForm.addEventListener('submit', function() {
      const runBtn = runForm.querySelector('button[type="submit"]');
      setButtonLoading(runBtn);
    });
  }

  // Test email form submission
  const testForm = document.getElementById('testForm');
  if (testForm) {
    testForm.addEventListener('submit', function(e) {
      const testEmailInput = document.querySelector('input[name="test_email"]');

      if (testEmailInput && !isValidEmail(testEmailInput.value)) {
        e.preventDefault();
        showFieldError(testEmailInput, 'Please enter a valid email address');
        showToast('Please enter a valid email address', 'danger');
        return false;
      }

      const testBtn = document.getElementById('testEmailBtn');
      setButtonLoading(testBtn);
    });
  }

  initLogViewer();

  // Add real-time validation for email fields
  const emailFields = document.querySelectorAll('input[type="email"], input[name*="email"]');
  emailFields.forEach(field => {
    field.addEventListener('blur', function() {
      if (this.value && !isValidEmail(this.value)) {
        showFieldError(this, 'Please enter a valid email address');
      } else {
        clearFieldError(this);
      }
    });

    field.addEventListener('input', function() {
      if (this.classList.contains('is-invalid') && isValidEmail(this.value)) {
        clearFieldError(this);
      }
    });
  });

  // Add real-time validation for URL fields
  const urlFields = document.querySelectorAll('input[name*="url"]');
  urlFields.forEach(field => {
    field.addEventListener('blur', function() {
      if (this.value && !isValidUrl(this.value)) {
        showFieldError(this, 'Please enter a valid URL');
      } else {
        clearFieldError(this);
      }
    });

    field.addEventListener('input', function() {
      if (this.classList.contains('is-invalid') && isValidUrl(this.value)) {
        clearFieldError(this);
      }
    });
  });
}

/**
 * Initialize the live log viewer on the settings page
 */
function initLogViewer() {
  const logOutput = document.getElementById('logOutput');
  if (!logOutput) return;

  const startBtn = document.getElementById('logStartBtn');
  const stopBtn = document.getElementById('logStopBtn');
  const clearBtn = document.getElementById('logClearBtn');
  const statusBadge = document.getElementById('logStatusBadge');
  const autoScrollToggle = document.getElementById('logAutoScroll');
  const logFileSelect = document.getElementById('logFileSelect');
  const logLevelFilter = document.getElementById('logLevelFilter');
  const logSearchFilter = document.getElementById('logSearchFilter');
  const logStats = document.getElementById('logStats');

  const POLL_INTERVAL_MS = 2000;
  const MAX_LINES = 500;
  let offset = 0;
  let pollTimer = null;
  let pendingLine = '';
  let firstFetch = true;
  let currentLogFile = logFileSelect?.value || 'app';
  let allLines = []; // Store all fetched lines for filtering
  let stats = { total: 0, errors: 0, warnings: 0 };

  const LOG_LEVEL_PRIORITY = { 'DEBUG': 0, 'INFO': 1, 'WARNING': 2, 'ERROR': 3 };

  const setStatus = (state, text) => {
    if (!statusBadge) return;
    statusBadge.textContent = text;
    statusBadge.classList.remove('bg-success', 'bg-warning', 'bg-secondary', 'bg-danger');
    if (state === 'connected') {
      statusBadge.classList.add('bg-success');
    } else if (state === 'paused') {
      statusBadge.classList.add('bg-secondary');
    } else if (state === 'error') {
      statusBadge.classList.add('bg-danger');
    } else {
      statusBadge.classList.add('bg-warning');
    }
  };

  const updateStats = () => {
    if (!logStats) return;
    logStats.textContent = `Lines: ${stats.total} | Errors: ${stats.errors} | Warnings: ${stats.warnings}`;
  };

  const getLogLevel = (line) => {
    if (line.includes('| ERROR |') || line.includes(' ERROR ')) return 'ERROR';
    if (line.includes('| WARNING |') || line.includes(' WARNING ')) return 'WARNING';
    if (line.includes('| INFO |') || line.includes(' INFO ')) return 'INFO';
    if (line.includes('| DEBUG |') || line.includes(' DEBUG ')) return 'DEBUG';
    return 'INFO'; // Default
  };

  const formatLine = (line) => {
    const level = getLogLevel(line);
    let className = 'log-line';
    if (level === 'ERROR') className += ' log-error';
    else if (level === 'WARNING') className += ' log-warning';
    else if (level === 'INFO') className += ' log-info';
    else if (level === 'DEBUG') className += ' log-debug';

    // Escape HTML
    const escaped = line.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return `<span class="${className}">${escaped}</span>`;
  };

  const filterAndRender = () => {
    const levelFilter = logLevelFilter?.value || '';
    const searchTerm = (logSearchFilter?.value || '').toLowerCase();
    const minPriority = levelFilter ? LOG_LEVEL_PRIORITY[levelFilter] : -1;

    const filteredLines = allLines.filter(line => {
      // Level filter
      if (minPriority >= 0) {
        const lineLevel = getLogLevel(line);
        if (LOG_LEVEL_PRIORITY[lineLevel] < minPriority) return false;
      }
      // Search filter
      if (searchTerm && !line.toLowerCase().includes(searchTerm)) return false;
      return true;
    });

    logOutput.innerHTML = filteredLines.map(formatLine).join('\n');

    if (autoScrollToggle?.checked) {
      logOutput.scrollTop = logOutput.scrollHeight;
    }
  };

  const trimLines = () => {
    if (allLines.length > MAX_LINES) {
      allLines = allLines.slice(-MAX_LINES);
    }
  };

  const appendLines = (lines) => {
    if (!lines || !lines.length) return;

    for (const line of lines) {
      allLines.push(line);
      stats.total++;
      const level = getLogLevel(line);
      if (level === 'ERROR') stats.errors++;
      else if (level === 'WARNING') stats.warnings++;
    }

    trimLines();
    filterAndRender();
    updateStats();
  };

  const resetState = () => {
    offset = 0;
    pendingLine = '';
    firstFetch = true;
    allLines = [];
    stats = { total: 0, errors: 0, warnings: 0 };
    logOutput.innerHTML = '';
    updateStats();
  };

  const fetchLogs = async () => {
    try {
      const fileParam = `file=${currentLogFile}`;
      const url = firstFetch
        ? `/api/admin/logs?${fileParam}&offset=tail`
        : `/api/admin/logs?${fileParam}&offset=${offset}`;
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`Unexpected response: ${response.status}`);
      }
      const data = await response.json();

      // Check if log file changed on server side
      if (data.log_file && data.log_file !== currentLogFile) {
        return; // Ignore stale response
      }

      const nextOffset = Number.isFinite(data.offset) ? data.offset : offset;
      const fileSize = Number.isFinite(data.file_size) ? data.file_size : null;

      // Detect file rotation
      if ((fileSize !== null && fileSize < offset) || nextOffset < offset) {
        resetState();
      }

      offset = nextOffset;
      let lines = Array.isArray(data.lines) ? data.lines : [];

      if (pendingLine) {
        if (lines.length) {
          lines[0] = `${pendingLine}${lines[0]}`;
        } else {
          lines = [pendingLine];
        }
        pendingLine = '';
      }
      if (!data.ends_with_newline && lines.length) {
        pendingLine = lines.pop();
      }
      appendLines(lines);
      setStatus('connected', 'Live');
      firstFetch = false;
    } catch (error) {
      setStatus('error', 'Disconnected');
    }
  };

  const startPolling = () => {
    if (pollTimer) return;
    fetchLogs();
    pollTimer = setInterval(fetchLogs, POLL_INTERVAL_MS);
    if (startBtn) startBtn.disabled = true;
    if (stopBtn) stopBtn.disabled = false;
  };

  const stopPolling = () => {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
    if (startBtn) startBtn.disabled = false;
    if (stopBtn) stopBtn.disabled = true;
    setStatus('paused', 'Paused');
  };

  const switchLogFile = () => {
    const newFile = logFileSelect?.value || 'app';
    if (newFile !== currentLogFile) {
      currentLogFile = newFile;
      resetState();
      if (pollTimer) {
        fetchLogs(); // Fetch immediately on switch
      }
    }
  };

  // Event listeners
  if (startBtn) startBtn.addEventListener('click', startPolling);
  if (stopBtn) stopBtn.addEventListener('click', stopPolling);
  if (clearBtn) {
    clearBtn.addEventListener('click', () => {
      allLines = [];
      stats = { total: 0, errors: 0, warnings: 0 };
      logOutput.innerHTML = '';
      pendingLine = '';
      updateStats();
    });
  }
  if (logFileSelect) logFileSelect.addEventListener('change', switchLogFile);
  if (logLevelFilter) logLevelFilter.addEventListener('change', filterAndRender);
  if (logSearchFilter) {
    let searchTimeout;
    logSearchFilter.addEventListener('input', () => {
      clearTimeout(searchTimeout);
      searchTimeout = setTimeout(filterAndRender, 150); // Debounce
    });
  }

  startPolling();
}

// ========== History Page Functions ==========

/**
 * Initialize history page functionality
 */
function initHistoryPage() {
  // Add smooth scrolling for links
  const userLinks = document.querySelectorAll('a[href*="history"]');
  userLinks.forEach(link => {
    link.addEventListener('click', function(e) {
      // Let the browser handle the navigation
      // Just add a visual effect
      this.style.transition = 'all 0.3s ease';
    });
  });

  // Enhance table rows with click-to-select
  const tableRows = document.querySelectorAll('table tbody tr');
  tableRows.forEach(row => {
    row.addEventListener('click', function() {
      this.style.backgroundColor = 'rgba(229, 160, 13, 0.1)';
      setTimeout(() => {
        this.style.backgroundColor = '';
      }, 1000);
    });
  });

  // Add filter animation
  const searchForm = document.querySelector('form[method="get"]');
  if (searchForm) {
    const submitBtn = searchForm.querySelector('button[type="submit"]');
    searchForm.addEventListener('submit', function() {
      if (submitBtn) {
        setButtonLoading(submitBtn);
      }
    });
  }
}

// ========== Subscriptions Page Functions ==========

/**
 * Initialize subscriptions page functionality
 */
function initSubscriptionsPage() {
  const globalOptOut = document.getElementById('globalOptOut');
  const showCheckboxes = document.querySelectorAll('input[name="show_optouts"]');

  // Disable show checkboxes when global opt-out is checked
  if (globalOptOut) {
    const updateShowCheckboxes = () => {
      showCheckboxes.forEach(checkbox => {
        checkbox.disabled = globalOptOut.checked;
        checkbox.parentElement.style.opacity = globalOptOut.checked ? '0.5' : '1';
      });
    };

    // Initial state
    updateShowCheckboxes();

    // Update on change
    globalOptOut.addEventListener('change', updateShowCheckboxes);
  }

  // Add confirmation for global opt-out
  const subscriptionForm = document.querySelector('form[method="POST"]');
  if (subscriptionForm && globalOptOut) {
    subscriptionForm.addEventListener('submit', function(e) {
      if (globalOptOut.checked) {
        const confirmed = confirm(
          'Are you sure you want to unsubscribe from ALL notifications? ' +
          'You will no longer receive any episode alerts.'
        );

        if (!confirmed) {
          e.preventDefault();
          return false;
        }
      }

      const submitBtn = subscriptionForm.querySelector('button[type="submit"]');
      setButtonLoading(submitBtn);
    });
  }

  // Server-side search is now handled via JavaScript navigation
  const showSearch = document.getElementById('showSearch');
  const searchButton = document.getElementById('searchButton');

  if (showSearch && searchButton) {
    const performSearch = () => {
      const searchQuery = showSearch.value.trim();
      const token = showSearch.dataset.token;
      const showInactive = showSearch.dataset.showInactive;

      // Build the URL with query parameters
      const params = new URLSearchParams({
        token: token,
        show_inactive: showInactive
      });

      if (searchQuery) {
        params.append('search', searchQuery);
      }

      // Navigate to the search URL
      window.location.href = `/subscriptions?${params.toString()}`;
    };

    // Handle search button click
    searchButton.addEventListener('click', performSearch);

    // Handle Enter key in search box
    showSearch.addEventListener('keypress', function(e) {
      if (e.key === 'Enter') {
        e.preventDefault();
        performSearch();
      }
    });
  }

  // Add select all / deselect all buttons for current page
  if (showCheckboxes.length > 5) {
    const buttonContainer = document.createElement('div');
    buttonContainer.className = 'mb-3 d-flex gap-2';
    buttonContainer.innerHTML = `
      <button type="button" class="btn btn-sm btn-outline-secondary" id="selectAllShows">
        Select All on Page
      </button>
      <button type="button" class="btn btn-sm btn-outline-secondary" id="deselectAllShows">
        Deselect All on Page
      </button>
    `;

    const showListParent = showCheckboxes[0].closest('.mb-3') || document.querySelector('h5');
    if (showListParent) {
      showListParent.parentElement.insertBefore(buttonContainer, showListParent.nextSibling);
    }

    document.getElementById('selectAllShows')?.addEventListener('click', function() {
      showCheckboxes.forEach(checkbox => {
        if (!checkbox.disabled) checkbox.checked = true;
      });
    });

    document.getElementById('deselectAllShows')?.addEventListener('click', function() {
      showCheckboxes.forEach(checkbox => {
        if (!checkbox.disabled) checkbox.checked = false;
      });
    });
  }
}

// ========== General Enhancements ==========

/**
 * Add smooth scrolling to anchor links
 */
function initSmoothScrolling() {
  document.querySelectorAll('a[href^="#"]').forEach(anchor => {
    anchor.addEventListener('click', function(e) {
      const targetId = this.getAttribute('href');
      if (targetId === '#') return;

      const targetElement = document.querySelector(targetId);
      if (targetElement) {
        e.preventDefault();
        targetElement.scrollIntoView({
          behavior: 'smooth',
          block: 'start'
        });
      }
    });
  });
}

/**
 * Add keyboard navigation support
 */
function initKeyboardNavigation() {
  // Add keyboard support for card interactions
  const cards = document.querySelectorAll('.card');
  cards.forEach(card => {
    card.setAttribute('tabindex', '0');
  });
}

/**
 * Initialize animations
 */
function initAnimations() {
  // Add intersection observer for fade-in animations
  const observerOptions = {
    threshold: 0.1,
    rootMargin: '0px 0px -50px 0px'
  };

  const observer = new IntersectionObserver((entries) => {
    entries.forEach(entry => {
      if (entry.isIntersecting) {
        entry.target.style.opacity = '1';
        entry.target.style.transform = 'translateY(0)';
      }
    });
  }, observerOptions);

  // Observe cards
  document.querySelectorAll('.card').forEach(card => {
    card.style.opacity = '0';
    card.style.transform = 'translateY(20px)';
    card.style.transition = 'opacity 0.6s ease, transform 0.6s ease';
    observer.observe(card);
  });
}

// ========== Page Router ==========

/**
 * Determine which page we're on and initialize accordingly
 */
function initPage() {
  const path = window.location.pathname;

  if (path === '/' || path.includes('settings')) {
    initSettingsPage();
  } else if (path.includes('log-viewer')) {
    initLogViewer();
  } else if (path.includes('history')) {
    initHistoryPage();
  } else if (path.includes('subscriptions')) {
    initSubscriptionsPage();
  }

  // Initialize general enhancements on all pages
  initSmoothScrolling();
  initKeyboardNavigation();
  initAnimations();
}

// ========== Initialize on DOM Ready ==========

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initPage);
} else {
  initPage();
}

// Export functions for testing or external use
if (typeof module !== 'undefined' && module.exports) {
  module.exports = {
    togglePasswordVisibility,
    isValidEmail,
    isValidUrl,
    showToast
  };
}
