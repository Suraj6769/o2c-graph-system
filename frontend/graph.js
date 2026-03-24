/**
 * Graph visualization using Cytoscape.js
 */

const API_BASE = "http://localhost:8000";

class GraphVisualizer {
    constructor(containerId) {
        this.container = document.getElementById(containerId);
        this.cy = null;
        this.selectedNodes = new Set();
        this.initializeCytoscape();
    }

    initializeCytoscape() {
        this.cy = cytoscape({
            container: this.container,
            style: this.getGraphStyle(),
            wheelSensitivity: 0.1,
            layout: {
                name: 'cola',
                animate: true,
                animationDuration: 500,
                randomize: false,
                maxSimulationTime: 4000,
                ungrabifyWhileSimulating: false,
                fit: true,
                padding: 30
            }
        });

        this.attachEventHandlers();
    }

    getGraphStyle() {
        return [
            {
                selector: 'node',
                style: {
                    'label': 'data(name)',
                    'text-valign': 'center',
                    'text-halign': 'center',
                    'background-color': '#667eea',
                    'color': '#fff',
                    'font-size': 12,
                    'width': 40,
                    'height': 40,
                    'border-width': 2,
                    'border-color': '#5568d3',
                    'text-wrap': 'wrap',
                    'text-max-width': 80,
                    'padding': 5
                }
            },
            {
                selector: 'node:selected',
                style: {
                    'background-color': '#764ba2',
                    'border-width': 3,
                    'border-color': '#fff'
                }
            },
            {
                selector: 'node:hover',
                style: {
                    'background-color': '#5568d3',
                    'cursor': 'pointer'
                }
            },
            {
                selector: 'edge',
                style: {
                    'target-arrow-shape': 'triangle',
                    'target-arrow-color': '#ccc',
                    'line-color': '#ccc',
                    'stroke-width': 2,
                    'label': 'data(relation_type)',
                    'font-size': 10,
                    'color': '#666',
                    'text-background-color': '#fff',
                    'text-background-opacity': 0.8,
                    'text-background-padding': '3px'
                }
            },
            {
                selector: 'edge:selected',
                style: {
                    'line-color': '#667eea',
                    'target-arrow-color': '#667eea',
                    'stroke-width': 3
                }
            }
        ];
    }

    attachEventHandlers() {
        this.cy.on('tap', 'node', (event) => {
            const node = event.target;
            this.handleNodeClick(node);
        });

        this.cy.on('tap', (event) => {
            if (event.target === this.cy) {
                this.clearSelection();
            }
        });
    }

    handleNodeClick(node) {
        const nodeId = node.id();
        
        if (this.selectedNodes.has(nodeId)) {
            this.selectedNodes.delete(nodeId);
            node.unselect();
        } else {
            this.selectedNodes.add(nodeId);
            node.select();
        }

        // Trigger event for other components to listen to
        window.dispatchEvent(new CustomEvent('nodeSelected', {
            detail: { nodeId, data: node.data() }
        }));
    }

    clearSelection() {
        this.cy.elements().unselect();
        this.selectedNodes.clear();
    }

    async loadGraph() {
        try {
            const response = await fetch(`${API_BASE}/graph`);
            if (!response.ok) throw new Error('Failed to load graph');
            
            const data = await response.json();
            this.addGraphData(data);
            this.updateStatistics(data);
        } catch (error) {
            console.error('Error loading graph:', error);
            this.showError('Failed to load graph data');
        }
    }

    addGraphData(graphData) {
        const elements = [];

        // Add nodes
        if (graphData.nodes) {
            graphData.nodes.forEach(([nodeId, nodeData]) => {
                elements.push({
                    data: {
                        id: String(nodeId),
                        name: nodeData.name || `Node ${nodeId}`,
                        type: nodeData.type,
                        description: nodeData.description
                    }
                });
            });
        }

        // Add edges
        if (graphData.edges) {
            graphData.edges.forEach(([source, target, edgeData]) => {
                elements.push({
                    data: {
                        id: `${source}-${target}`,
                        source: String(source),
                        target: String(target),
                        relation_type: edgeData.relation_type || 'related',
                        weight: edgeData.weight
                    }
                });
            });
        }

        this.cy.add(elements);
        this.cy.layout({ name: 'cola', animate: true, animationDuration: 500 }).run();
    }

    updateStatistics(graphData) {
        const entityCount = graphData.nodes ? graphData.nodes.length : 0;
        const relationshipCount = graphData.edges ? graphData.edges.length : 0;

        document.getElementById('entity-count').textContent = entityCount;
        document.getElementById('relationship-count').textContent = relationshipCount;
    }

    highlightPath(nodes) {
        // Clear previous highlights
        this.cy.elements().removeClass('highlighted');

        const nodeIds = nodes.map(n => String(n));
        const nodesToHighlight = this.cy.nodes().filter(node => 
            nodeIds.includes(node.id())
        );

        nodesToHighlight.addClass('highlighted');
        
        // Fit to highlighted nodes
        if (nodesToHighlight.length > 0) {
            this.cy.fit(nodesToHighlight, 50);
        }
    }

    showError(message) {
        const errorContainer = document.getElementById('error-container');
        if (errorContainer) {
            errorContainer.innerHTML = `<div class="error">${message}</div>`;
            setTimeout(() => {
                errorContainer.innerHTML = '';
            }, 5000);
        }
    }
}

// Initialize graph on page load
let graphVisualizer;

document.addEventListener('DOMContentLoaded', () => {
    graphVisualizer = new GraphVisualizer('graph-container');
    graphVisualizer.loadGraph();
});
