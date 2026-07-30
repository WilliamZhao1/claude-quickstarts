# Computer Use Agent System Architecture

**Author:** William Zhao

## Architecture Overview

This repository provides a web interface and backend routing architecture built to manage sandboxed agent operations through the Anthropic API. 

### Frontend Interface
The system relies on a unified dashboard containing a task history panel, a centralized VNC integration frame, and a real-time WebSocket chat layer for inputting text queries and streaming task logs. The frontend ensures that multiple active sessions can be tracked simultaneously while enforcing secure, low-latency UI updates.

### Backend API & Execution Loop
The backend architecture operates using FastAPI paired with SQLAlchemy for SQLite operations.
*   **Database Persistance:** Maintains consistent event records, active container IDs, host port allocations, and complete historical chat flows utilizing `MessageModel` and `SessionModel` definitions.
*   **Docker Container Management:** Every new session request directly allocates an isolated `computer-use-demo:local` Docker container. The system automatically acquires open host TCP ports native to the environment to bind VNC exposure without race conditions. 
*   **Agent Abstraction:** The backend integrates an asynchronous task loop targeting the `claude-opus-4-8` model using the `computer-use-2025-11-24` tool configuration. Any requested tool calls (e.g., text edits or bash executions) are seamlessly serialized and injected into the target container environment using Python scripts embedded inside tarballs via the Docker daemon.


### Video DEMO
https://streamable.com/9hsukz 