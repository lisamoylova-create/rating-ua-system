// Main JavaScript file for the application

document.addEventListener('DOMContentLoaded', function() {
    // Initialize tooltips
    var tooltipTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="tooltip"]'));
    var tooltipList = tooltipTriggerList.map(function (tooltipTriggerEl) {
        return new bootstrap.Tooltip(tooltipTriggerEl);
    });

    // Initialize popovers
    var popoverTriggerList = [].slice.call(document.querySelectorAll('[data-bs-toggle="popover"]'));
    var popoverList = popoverTriggerList.map(function (popoverTriggerEl) {
        return new bootstrap.Popover(popoverTriggerEl);
    });

    // Auto-hide alerts after 5 seconds
    setTimeout(function() {
        var alerts = document.querySelectorAll('.alert:not(.alert-permanent)');
        alerts.forEach(function(alert) {
            var bsAlert = new bootstrap.Alert(alert);
            bsAlert.close();
        });
    }, 5000);

    // Confirm dialogs for destructive actions
    document.querySelectorAll('[data-confirm]').forEach(function(element) {
        element.addEventListener('click', function(e) {
            const message = this.getAttribute('data-confirm');
            if (!confirm(message)) {
                e.preventDefault();
                return false;
            }
        });
    });

    // Format numbers in tables
    formatNumbers();
    
    // Enable search functionality if present
    initializeSearch();
});

function formatNumbers() {
    // Format large numbers with thousands separators
    document.querySelectorAll('.format-number').forEach(function(element) {
        const number = parseInt(element.textContent);
        if (!isNaN(number)) {
            element.textContent = number.toLocaleString('uk-UA');
        }
    });
}

function initializeSearch() {
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        let timeout;
        searchInput.addEventListener('input', function() {
            clearTimeout(timeout);
            timeout = setTimeout(() => {
                performSearch(this.value);
            }, 300);
        });
    }
}

function performSearch(query) {
    // Simple client-side search implementation
    const rows = document.querySelectorAll('tbody tr[data-searchable]');
    rows.forEach(function(row) {
        const text = row.textContent.toLowerCase();
        const searchQuery = query.toLowerCase();
        
        if (text.includes(searchQuery) || searchQuery === '') {
            row.style.display = '';
        } else {
            row.style.display = 'none';
        }
    });
}

// Utility function to show loading spinner
function showLoading(elementId) {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = `
            <div class="text-center py-3">
                <div class="spinner-border" role="status">
                    <span class="visually-hidden">Завантаження...</span>
                </div>
            </div>
        `;
    }
}

// Utility function to show error message
function showError(elementId, message) {
    const element = document.getElementById(elementId);
    if (element) {
        element.innerHTML = `
            <div class="alert alert-danger" role="alert">
                <i class="bi bi-exclamation-triangle"></i>
                ${message}
            </div>
        `;
    }
}

// Function to copy text to clipboard
function copyToClipboard(text) {
    navigator.clipboard.writeText(text).then(function() {
        // Show success toast
        showToast('Скопійовано до буферу обміну', 'success');
    }).catch(function(err) {
        console.error('Could not copy text: ', err);
        showToast('Помилка копіювання', 'danger');
    });
}

// Function to show toast notifications
function showToast(message, type = 'info') {
    const toastContainer = document.getElementById('toast-container') || createToastContainer();
    
    const toastId = 'toast-' + Date.now();
    const toast = document.createElement('div');
    toast.id = toastId;
    toast.className = `toast align-items-center text-white bg-${type} border-0`;
    toast.setAttribute('role', 'alert');
    toast.innerHTML = `
        <div class="d-flex">
            <div class="toast-body">
                ${message}
            </div>
            <button type="button" class="btn-close btn-close-white me-2 m-auto" data-bs-dismiss="toast"></button>
        </div>
    `;
    
    toastContainer.appendChild(toast);
    
    const bsToast = new bootstrap.Toast(toast);
    bsToast.show();
    
    // Remove toast after it's hidden
    toast.addEventListener('hidden.bs.toast', function() {
        toast.remove();
    });
}

function createToastContainer() {
    const container = document.createElement('div');
    container.id = 'toast-container';
    container.className = 'toast-container position-fixed bottom-0 end-0 p-3';
    container.style.zIndex = '1055';
    document.body.appendChild(container);
    return container;
}

// Function to validate file uploads
function validateFileUpload(input, allowedTypes = ['.xlsx', '.xls', '.csv'], maxSize = 16 * 1024 * 1024) {
    const file = input.files[0];
    if (!file) return true;
    
    // Check file size
    if (file.size > maxSize) {
        showToast(`Файл занадто великий. Максимальний розмір: ${(maxSize / 1024 / 1024).toFixed(0)} МБ`, 'danger');
        input.value = '';
        return false;
    }
    
    // Check file type
    const fileExtension = '.' + file.name.split('.').pop().toLowerCase();
    if (!allowedTypes.includes(fileExtension)) {
        showToast(`Недозволений тип файлу. Дозволені: ${allowedTypes.join(', ')}`, 'danger');
        input.value = '';
        return false;
    }
    
    return true;
}

// Export functions for global use
window.copyToClipboard = copyToClipboard;
window.showToast = showToast;
window.showLoading = showLoading;
window.showError = showError;
window.validateFileUpload = validateFileUpload;
