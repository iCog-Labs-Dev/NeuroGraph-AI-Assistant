# NeuroGraph AI Assistant - Complete Setup & Usage Guide

## Project Overview

The NeuroGraph AI Assistant is a **two-step pipeline** for discovering patterns and motifs in graph data:

1.  **Step 1: Generate Graph** - Upload CSV files and generate a NetworkX graph (returns `job_id`).
2.  **Step 2: Mine Patterns** - Use the `job_id` to discover patterns with custom configuration.

### System Components

-   **Custom AtomSpace Builder**: Converts CSV data into NetworkX graphs.
-   **Neural Subgraph Miner**: Discovers motifs and patterns.
-   **Integration Service**: Orchestrates the workflow and provides REST APIs.

---

##  Complete Setup Instructions (Step-by-Step)

Follow these steps precisely to set up the system on a new server or local machine.

### Prerequisites
-   **Docker** and **Docker Compose** installed.
-   **Git** installed.

### Step 1: Clone the Repository
Clone the repository and ensure submodules are downloaded.

```bash
# Clone the main repository
git clone https://github.com/NeuroGraph-AI-Assistant.git
cd NeuroGraph-AI-Assistant

# Initialize and update submodules (CRITICAL STEP)
git submodule update --init --recursive
```

### Step 2: Configure Environment Variables
The system uses a **single configuration file** in the root directory.

1.  Copy the example `env` file:
    ```bash
    cp .env.example .env
    ```

2.  Open `.env` in a text editor (e.g., `nano .env`) and set your keys:
    *   **LLM_API_KEY**: Your OpenAI or Anthropic key.
    *   **NEO4J_PASSWORD**: Default is `atomspace123`.
    *   **(Optional) Ports**: Change `ATOMSPACE_PORT` here if port 8000 is busy.
   

### Step 3: Prepare Submodule Build (Build Fix)
*Note: The AtomSpace Builder requires a local `.env` file to exist during the build process.*

Run this command to create the necessary dummy file:

```bash
# Create a dummy .env file for the submodule build
cp .env.example submodules/custom-atomspace-builder/.env
```

### Step 4: Build and Start the System
Run the unified Docker Compose command.

```bash
# Build and start in detached mode (background)
docker compose up --build -d
```

### Step 5: Verify Services
Check the status of the containers:
```bash
docker compose ps
```
**Expected Output:** All 5 services (integration-service, neural-miner, atomspace-api, neo4j, hugegraph) should be "Up".

---

## Troubleshooting & Common Errors

### 1. Error 500: "Name or service not known" (Annotation Service)
**Issue:** The system defaults to looking for an optional "Annotation Service" that doesn't exist.
**Fix:**
Edit your `.env` file and make sure `ANNOTATION_SERVICE_URL` is empty.
```bash
# Correct:
ANNOTATION_SERVICE_URL=
# Incorrect:
ANNOTATION_SERVICE_URL=http://annotation-service:6000
```
Then restart: `docker compose up -d`

### 2. Error: "Bind for 0.0.0.0:8000 failed" (Port Conflict)
**Issue:** Port 8000 is already used by another application (or an old version of this app).
**Fix:**
*   **Option A (Kill Old Process):** Run `sudo lsof -i :8000` to find the PID, then `sudo kill -9 <PID>`.
*   **Option B (Change Port):** Edit `.env` and set `ATOMSPACE_PORT=8001`.

### 3. Error: "Conflict. The container name ... is already in use"
**Issue:** Old containers from manual runs are blocking the new ones.
**Fix (The Nuclear Option):**
```bash
docker rm -f $(docker ps -aq)
docker compose up --build -d
```

---

##  API Guidelines

**Base URL:** `http://localhost:9000`

### 1️ Step 1: Generate Graph
**Endpoint:** `POST /api/generate-graph`
Upload your CSV files to create a graph.

```bash
curl -X POST "http://localhost:9000/api/generate-graph" \
  -F "files=@nodes.csv" \
  -F "files=@edges.csv" \
  -F "config=$(cat config.json)" \
  -F "schema_json=$(cat schema.json)" \
  -F "writer_type=networkx" \
  -F "graph_type=directed"
```

### 2️ Step 2: Mine Patterns
**Endpoint:** `POST /api/mine-patterns`
Use the `job_id` from Step 1.

```bash
curl -X POST "http://localhost:9000/api/mine-patterns" \
  -F "job_id=abc-123" \
  -F "min_pattern_size=3"
```
