# System Architecture & Agents Specification (For AI Codex)

## 📌 1. System Overview
**Project Name:** Avideo
**Description:** An automated, local-first media metadata scraping and AI translation system.
**Architecture Pattern:** Multi-Agent Producer-Consumer model communicating exclusively via a SQLite Database (Acting as the State Machine and Message Queue).
**Core Rule:** Agents MUST NOT communicate with each other directly. They only read from and write to the Database Access Layer (DAL).

---

## 🤖 2. Definitions

###  1: Scanner (`core/scanner.py`)
* **Role:** Producer.
* **Responsibility:** Scan local directories, extract media codes (e.g., `SSNI-432`) using regex, and register them into the database.
* **Input:** Local file paths (from `config.MEDIA_LIBRARIES`).
* **Output:** Insert records to DAL with `scrape_status = 'PENDING'`.
* **AI Coding Constraint:** Must strip custom local suffixes (e.g., `-C`, `-U`, `-R`) before extracting the core code.

### 2: Auto-Scraper (`core/scraper/`)
* **Role:** Consumer & Web Automation Worker.
* **Responsibility:** Fetch `PENDING` codes from DAL, scrape JavDB using Playwright, bypass Cloudflare/Age Gates, and download media assets.
* **Key Behaviors:**
    * Must use `playwright.sync_api`.
    * Must implement fallback search (e.g., if `IPX-534` fails, try `IPX 534`).
    * Must open a new browser context/page to scrape Actor Profiles (fetching `aliases` and `avatar_path`).
    * Must implement random sleep (15-30s) between tasks to prevent IP bans.
* **Output:** Structured Python Dictionary containing metadata, passed to DAL for updating.

###  3: AI Translator (`core/translator/`)
* **Role:** NLP Data Processor.
* **Responsibility:** Translate Japanese titles to Chinese using a local Ollama instance.
* **Core Logic (Entity Stripping):** * **CRITICAL:** Before sending text to the LLM, the agent MUST physically remove all known actor names and aliases from the `title_jp` string using `str.replace()` and regex.
    * Must sort actor names by length (descending) before replacing to avoid partial string matches.
* **Fallback:** If the Ollama API connection fails, return the original `title_jp`.

###  4: Data Access Layer (DAL) (`dal/db_manager.py`)
* **Role:** The ONLY module allowed to execute SQL queries.
* **Responsibility:** Manage SQLite connections, handle transactions, and maintain Many-to-Many relationships.
* **AI Coding Constraint:** * Other modules (Scraper, Translator) MUST NOT import `sqlite3`.
    * Always use parameterized queries (`?`) to prevent SQL injection.
    * Map Python `None` to SQLite `NULL`.

---

## 🗄️ 3. Database Schema Reference (SQLite)

Code generation must strictly adhere to this schema:

```sql
-- Main Video Table
CREATE TABLE videos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT NOT NULL,                 
    title_jp TEXT,                      
    title_zh TEXT,                      
    release_date TEXT,                  
    duration TEXT,                      
    maker TEXT,                         
    publisher TEXT,                     
    series TEXT,                        
    score REAL,                         
    cover_path TEXT,                    
    original_file_path TEXT UNIQUE NOT NULL, 
    scrape_status TEXT DEFAULT 'PENDING', -- ENUM: 'PENDING', 'SUCCESS', 'FAILED'
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Actors Table
CREATE TABLE actors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    aliases TEXT,
    avatar_path TEXT
);

-- Tags Table
CREATE TABLE tags (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL
);

-- Link Tables (Many-to-Many)
CREATE TABLE video_actor_link (video_id INTEGER, actor_id INTEGER, UNIQUE(video_id, actor_id));
CREATE TABLE video_tag_link (video_id INTEGER, tag_id INTEGER, UNIQUE(video_id, tag_id));
```
---

## 📄 4. Global Coding Guidelines for AI Codex
* Type Hinting: All new functions must include Python type hints 

* Logging: Use the standard Python logging module. Do not use print(). Log levels: INFO for state changes, WARNING for retries, ERROR for exceptions.

* Graceful Degradation: If a specific DOM element is missing during scraping, do not crash. Catch the exception, log a warning, assign None, and continue.

* Language: Python 3.10+.

## 🌐 5. Web Presentation Layer (Frontend & Backend)

The system requires a web interface that replicates the core browsing and filtering experience of a professional media catalog (e.g., JavDB). This is handled by two closely coupled agents.

###  5: Web API Server (Backend)
* **Role:** RESTful API Provider.
* **Suggested Tech Stack:** `FastAPI` (Python) for high performance and auto-generated Swagger docs, or `Flask`.
* **Responsibility:** Serve data from the SQLite DAL to the frontend and serve static assets (covers, avatars).
* **AI Coding Constraints & Required Endpoints:**
    * **MUST** interact with the database ONLY through parameterized SQL queries (read-only for the web context).
    * `GET /api/videos`: Return a paginated list of videos (descending order by `release_date` or `created_at`). Support query parameters: `?page=1&limit=24`.
    * `GET /api/videos/{code}`: Return full details of a specific video, performing SQL `JOIN`s to include associated `actors` and `tags`.
    * `GET /api/search`: Multi-field search (query against `code`, `title_jp`, `title_zh`, `maker`).
    * `GET /api/actors/{id}`: Return actor details (name, aliases, avatar) AND a list of all videos associated with this actor.
    * `GET /api/tags/{id}`: Return a list of all videos associated with a specific tag.
    * **Static Mount:** Must mount `/static/covers` and `/static/avatars` to serve local image assets.

###  6: Web UI (Frontend)
* **Role:** Single Page Application (SPA) or Server-Side Rendered (SSR) Client.
* **Suggested Tech Stack:** `Vue 3` + `Tailwind CSS` (or React/Next.js) for modern, responsive masonry/grid layouts.
* **Responsibility:** Consume the Web API Server and render the UI.
* **Core UI Components to Generate:**
    1.  **Navbar & Search Bar:** Global search input and navigation links (Home, Actors, Tags).
    2.  **Video Grid (Home/List View):** A responsive grid (CSS Grid or Masonry). Each item is a `VideoCard` component showing the `cover_path` (image), `code`, `title_zh` (or `title_jp` if untranslated), and `score`.
    3.  **Video Detail Page:**
        * Left Column: High-resolution cover image.
        * Right Column: Metadata panel (Code, Release Date, Duration, Maker).
        * Tags section (clickable pills routing to Tag View).
        * Actors section (avatars + names, clickable routing to Actor View).
        * *Special Local Feature:* A "Play Local File" button utilizing the `original_file_path`.
    4.  **Actor Profile Page:** Shows the actor's `avatar_path`, `name`, `aliases`, and a Video Grid of all their works.

### 7: Utils
* **Role:** all helpers put in `./utils/` folder

* **AI Coding Constraints:**
    * Design MUST be responsive (Mobile & Desktop friendly) using utility classes (e.g., Tailwind).
    * Use clean, minimalist aesthetics (dark mode preferred for media catalogs).
    * Handle empty states gracefully (e.g., display a placeholder image if `cover_path` or `avatar_path` is `NULL`).