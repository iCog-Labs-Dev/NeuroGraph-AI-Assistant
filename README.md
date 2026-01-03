# NeuroGraph AI Assistant

The **NeuroGraph AI Assistant** is a comprehensive platform designed for advanced graph processing, neural pattern mining, and interactive knowledge graph exploration. It integrates multiple specialized services to transform raw data into actionable insights through a unified pipeline.

## System Architecture

The system is composed of:

*   **Integration Service**: The central orchestrator that manages workflows, streams data between services, and provides a unified REST API for the frontend.
*   **Custom AtomSpace Builder**: A powerful graph processing engine that ingests CSV/JSON data and transforms it into NetworkX graphs, MeTTa formats, or Neo4j databases.
*   **Neural Subgraph Miner**: A specialized mining engine that uses neural search strategies (Greedy, MCTS) to discover frequent patterns and motifs within the generated graphs.
*   **Annotation Tool (Frontend)**: A modern, interactive React/Remix web interface for uploading data, visualizing Knowledge Graphs (KG), running mining jobs, and exploring results.
*   **Annotation Query Backend**: A supporting backend service that handles graph queries and history management for the visualization tool.

## Features

### Core Capabilities
*   **End-to-End Pipeline**: A seamless workflow from raw CSV upload to visualized graph patterns,getting their insights.
*   **Neural Mining**: Advanced subgraph mining using neural networks to guide the search for significant motifs.
*   **Interactive Visualization**: Explore your Knowledge Graph (KG) with an interactive, node-link diagram interface.
*   **Multi-Format Support**: Generate outputs in NetworkX, MeTTa, and Neo4j formats.
*   **Job Management**: Track and manage your data ingestion and mining jobs with persistent history.

### Frontend Experience
*   **Data Import**: Easy-to-use drag-and-drop interface for uploading CSV nodes and edges.
*   **Graph Exploration**: Visualizers for graph schema, top entities, and connectivity stats.
*   **Mining Configuration**: Fine-grained control over mining parameters (Pattern Size, Search Strategy, Sampling Method).
*   **Results Dashboard**: Downloadable reports and visualized mining outcomes.

## Installation & Setup

### 1. Clone the Repository

Clone the repository and ensure all submodules are initialized.

```bash
git clone https://github.com/kedistS/NeuroGraph-AI-Assistant.git
cd NeuroGraph-AI-Assistant

# Initialize and update submodules
git submodule update --init --recursive
```

2. **Configure environment variables:**
   ```bash
   cp .env.example .env
   ```
   
   Edit `.env` and set your Neo4j password (optional, defaults work fine):
   ```bash
   NEO4J_PASSWORD=your_secure_password
   ```


## Usage: End-to-End Pipeline Guide

This section describes the standard workflow for processing data and exploring graph motifs through the NeuroGraph AI Assistant.

### 1. System Initialization
Start all core services and databases using Docker Compose:
```bash
docker-compose up --build
```
Once the containers are active, access the **Frontend Dashboard** at [http://localhost:3000](http://localhost:3000).

### 2. Data Ingestion & Transformation
1. Navigate to the **Import** page.
2. Upload your **Nodes CSV** and **Edges CSV** files.
3. Select **NetworkX** as the target format.
4. The system will automatically generate the graph in **MORK format** in the background. 
5. Provide a name for your file and save it; the system handles job tracking automatically without requiring manual ID management.

### 3. Graph Analytics
Upon successful ingestion, the interface will automatically transition to the **Analytics View**. Here, you can explore:
*   Graph connectivity and density statistics.
*   Node and edge type distributions.
*   Schema visualizations for the uploaded Knowledge Graph.

### 4. Neural Motif Mining
1. From the analytics dashboard, click on **Mine Patterns**.
2. Configure your mining parameters (e.g., search strategy, pattern size).
3. The neural engine will discover frequent motifs and generate downloadable results.

### 5. AI-Powered Motif Interpretation
1. Download the mining results and open the interactive **HTML plots**.
2. Within the plot interface, provide your **Gemini API Key** to enable the **Instance-Aware Interpreter**.
3. Interact with the chat interface to receive structural insights and expert-level explanations for specific motifs.

### 6. Advanced Annotation Workflow
1. Navigate from the plots to the **Annotation Service** by clicking the **Annotation** button.
2. Build sophisticated queries for your data (since the MORK format is already generated and cached).
3. Use the **Upload** button to add specific files needed for your targeted annotation tasks.

---

## Service Documentation & API Access

For developers and advanced users, all services expose RESTful APIs and interactive documentation.

### Core Service Endpoints
| Service | Host URL | Documentation (Swagger/UI) | Purpose |
| :--- | :--- | :--- | :--- |
| **Integration Service** | [http://localhost:9000](http://localhost:9000) | [/docs](http://localhost:9000/docs) | Orchestration & Unified API |
| **AtomSpace Builder** | [http://localhost:8000](http://localhost:8000) | [/docs](http://localhost:8000/docs) | Data Ingestion & MeTTa Conversion |
| **Annotation Backend** | [http://localhost:8001](http://localhost:8001) | N/A | Query Management |
| **Neo4j DB** | [http://localhost:7474](http://localhost:7474) | [Browser interface](http://localhost:7474) | Graph Storage & Exploration |

### Interacting with APIs
You can use the **Integration API Swagger UI** to trigger pipeline steps programmatically or to check the status of active processes across the entire stack.

## Troubleshooting

### Common Issues

*   **"Container exited with code 1"**: Check logs using `dockerlogs <container_name>`.
*   **"Submodule not found"**: Run `git submodule update --init --recursive` again.

### Useful Commands

```bash
# View logs for all services
docker-compose logs -f

# Restart a specific service
docker-compose restart integration-service

# Rebuild a specific service
docker-compose up -d --build --no-deps annotation-tool
```
