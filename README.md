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
*   **End-to-End Pipeline**: A seamless workflow from raw CSV upload to visualized graph patterns.
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
git clone https://github.com/Samrawitgebremaryam/NeuroGraph-AI-Assistant.git
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

3. **Start the entire stack (Backend + Frontend + Databases):**
   ```bash
   docker-compose up --build
   ```
   
   *Note: The first build may take 5-10 minutes as it compiles all services.*

4. **Access the application:**
   - **Frontend Dashboard**: [http://localhost:3000](http://localhost:3000)
   - **Integration API**: [http://localhost:9000/docs](http://localhost:9000/docs)
   - **Neo4j Browser**: [http://localhost:7474](http://localhost:7474)

### Alternative: Local Frontend Development

If you want to develop the frontend locally (with hot-reload) while keeping the backend in Docker:

1. **Start only the backend services:**
   ```bash
   docker-compose up neo4j hugegraph atomspace-api-dev neural-miner integration-service annotation-backend mongodb redis
   ```

2. **Run the frontend locally:**
   ```bash
   cd submodules/annotation-tool
   cp .env.example .env  # Only needed first time
   npm install
   npm run dev
   ```
   
   The frontend will be available at [http://localhost:5173](http://localhost:5173) (Vite dev server).

## Usage

### Accessing the Application

Once all services are up (check with `docker-compose ps`), access the web interface:

*   **Frontend Dashboard**: [http://localhost:3000](http://localhost:3000)
    *   Navigate here to start importing data and running mining jobs.

### Typical Workflow

1.  **Import Data**:
    *   Go to the **Import** page.
    *   Upload your Nodes CSV and Edges CSV.
    *   Submit to generate the graph.
2.  **View Statistics**:
    *   Once generated, you'll see a dashboard with Graph Statistics (Node counts, Edge counts, Schema).
    *   **Copy the Job ID** from the success card.
3.  **Mine Patterns**:
    *   Go to the **Mine** page.
    *   Paste the Job ID.
    *   Configure mining parameters (e.g., Min Pattern Size=3, Strategy=Greedy).
    *   Click "Start Mining".
4.  **Analyze Results**:
    *   Download the results ZIP file containing the mined patterns and instances.

## Service Endpoints

*   **Integration API**: [http://localhost:9000/docs](http://localhost:9000/docs) (Swagger UI)
*   **AtomSpace Builder API**: [http://localhost:8000/docs](http://localhost:8000/docs)
*   **Annotation Backend**: [http://localhost:8001](http://localhost:8001)
*   **Neo4j Browser**: [http://localhost:7474](http://localhost:7474) (User: `neo4j`, Pass: `atomspace123`)

## Troubleshooting

### Common Issues

*   **"Frontend cannot connect to backend"**: Ensure you created the `.env` file in `submodules/annotation-tool` and that the URLs point to the correct ports exposed by Docker.
*   **"Container exited with code 1"**: Check logs using `dockerlogs <container_name>`.
*   **"Submodule not found"**: Run `git submodule update --init --recursive` again.

### Useful Commands

```bash
# View logs for all services
docker-compose logs -f

# Restart a specific service
docker-compose restart integration-service

# Rebuild a specific service
docker-compose up -d --build --no-deps annotation-frontend
```
