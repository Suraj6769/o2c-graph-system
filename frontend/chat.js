/**
 * Chat interface and query handling
 */

const API_BASE = "http://localhost:8000";

class ChatManager {
    constructor() {
        this.chatContainer = document.getElementById('chat-messages');
        this.queryInput = document.getElementById('query-input');
        this.sendButton = document.getElementById('send-btn');
        this.errorContainer = document.getElementById('error-container');
        
        this.attachEventHandlers();
    }

    attachEventHandlers() {
        this.sendButton.addEventListener('click', () => this.sendQuery());
        this.queryInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.sendQuery();
            }
        });
    }

    async sendQuery() {
        const query = this.queryInput.value.trim();
        if (!query) return;

        // Add user message to chat
        this.addMessage(query, 'user');
        this.queryInput.value = '';
        this.sendButton.disabled = true;

        try {
            // Show loading indicator
            this.addMessage('Processing...', 'assistant', true);

            // Send query to backend
            const response = await fetch(`${API_BASE}/query`, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({
                    query: query,
                    top_k: 5
                })
            });

            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }

            const data = await response.json();

            // Remove loading message
            const messages = this.chatContainer.querySelectorAll('.message');
            if (messages.length > 0) {
                messages[messages.length - 1].remove();
            }

            // Add assistant response
            this.addMessage(data.explanation, 'assistant');

            // Display results
            this.displayQueryResults(data.results);

            // Clear error if any
            this.errorContainer.innerHTML = '';

        } catch (error) {
            console.error('Query error:', error);
            
            // Remove loading message
            const messages = this.chatContainer.querySelectorAll('.message');
            if (messages.length > 0) {
                messages[messages.length - 1].remove();
            }

            // Show error
            const errorMsg = error.message || 'Failed to process query';
            this.errorContainer.innerHTML = `<div class="error">${errorMsg}</div>`;
            this.addMessage(`Error: ${errorMsg}`, 'assistant');

        } finally {
            this.sendButton.disabled = false;
            this.queryInput.focus();
        }
    }

    addMessage(text, sender, isLoading = false) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${sender}`;
        
        if (isLoading) {
            messageDiv.innerHTML = `<span class="loading"></span> ${text}`;
        } else {
            messageDiv.textContent = text;
        }
        
        this.chatContainer.appendChild(messageDiv);
        this.chatContainer.scrollTop = this.chatContainer.scrollHeight;
    }

    displayQueryResults(results) {
        if (!results || results.length === 0) {
            this.addMessage('No results found.', 'assistant');
            return;
        }

        // Create results message
        const resultsSummary = `Found ${results.length} result${results.length !== 1 ? 's' : ''}:`;
        this.addMessage(resultsSummary, 'assistant');

        // Display each result
        results.forEach((result, index) => {
            const resultText = this.formatResult(result);
            const bullet = `• ${resultText}`;
            this.addMessage(bullet, 'assistant');
        });
    }

    formatResult(result) {
        // Format a single result for display
        if (result.name) {
            return `${result.name} (${result.type || 'unknown'})`;
        }
        
        const keys = Object.keys(result).slice(0, 2);
        return keys.map(k => `${k}: ${result[k]}`).join(', ');
    }
}

// Initialize chat manager on page load
let chatManager;

document.addEventListener('DOMContentLoaded', () => {
    chatManager = new ChatManager();
});

// Listen for node selection events from graph
window.addEventListener('nodeSelected', (event) => {
    const { nodeId, data } = event.detail;
    const message = `Selected: ${data.name} (${data.type})`;
    chatManager.addMessage(message, 'assistant');
});
