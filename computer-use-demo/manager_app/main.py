import uuid
import docker
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
import anthropic

from fastapi.responses import FileResponse
from manager_app.database import init_db, AsyncSessionLocal, SessionModel, MessageModel

app = FastAPI(title="Computer Use API")
docker_client = docker.from_env()
client = anthropic.AsyncAnthropic()

@app.get("/")
async def serve_frontend():
    # The Docker container's WORKDIR is /app, so this path is relative to that
    return FileResponse("frontend/index.html")

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

@app.on_event("startup")
async def startup():
    await init_db()

class SessionCreateResponse(BaseModel):
    session_id: str
    vnc_port: int

@app.post("/sessions", response_model=SessionCreateResponse)
async def create_session(db: AsyncSession = Depends(get_db)):
    session_id = str(uuid.uuid4())
    
    # 1. Provision an isolated sandbox container for this specific session
    # We bind the container's 6080 (noVNC) to an ephemeral port on the host
    container = docker_client.containers.run(
        "anthropic-sandbox:latest",
        detach=True,
        ports={'6080/tcp': None},
        environment={"WIDTH": "1024", "HEIGHT": "768"}
    )
    
    # Reload to get the dynamically assigned host port for VNC
    container.reload()
    vnc_port = int(container.ports['6080/tcp'][0]['HostPort'])
    
    # 2. Persist to DB
    new_session = SessionModel(
        id=session_id, 
        container_id=container.id, 
        vnc_port=vnc_port
    )
    db.add(new_session)
    await db.commit()
    
    return {"session_id": session_id, "vnc_port": vnc_port}

@app.websocket("/sessions/{session_id}/stream")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    
    # Verify session
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(SessionModel).filter(SessionModel.id == session_id))
        session_data = result.scalars().first()
        if not session_data:
            await websocket.close(code=1008)
            return

    try:
        while True:
            # Receive user command
            data = await websocket.receive_json()
            user_msg = data.get("prompt")
            
            # Save user msg to DB
            async with AsyncSessionLocal() as db:
                db.add(MessageModel(session_id=session_id, role="user", content=user_msg))
                await db.commit()
            
            # Stream initial thought
            await websocket.send_json({"type": "progress", "content": "Thinking..."})
            
            # Execute Agent Loop (Simplified for demonstration)
            # In production, this integrates Anthropic's beta tools and routes the actual tool 
            # execution into the specific `session_data.container_id` via docker exec or HTTP.
            await run_agent_loop(websocket, session_id, session_data.container_id, user_msg)
            
    except WebSocketDisconnect:
        print(f"Client disconnected from session {session_id}")

async def run_agent_loop(websocket: WebSocket, session_id: str, container_id: str, prompt: str):
    """
    Executes the LLM ReAct loop. Tool calls are routed into the isolated Docker container.
    """
    system_prompt = "You are a computer use agent. You have access to a sandboxed Ubuntu environment."
    messages = [{"role": "user", "content": prompt}]
    
    # Example Anthropic Beta API call
    response = await client.beta.messages.create(
        model="claude-3-5-sonnet-20241022",
        max_tokens=1024,
        system=system_prompt,
        messages=messages,
        # tools=[... computer, bash, edit tools ...]
    )

    # Stream agent actions back to UI
    for content_block in response.content:
        if content_block.type == "text":
            await websocket.send_json({"type": "message", "role": "assistant", "content": content_block.text})
            
            # Persist assistant message
            async with AsyncSessionLocal() as db:
                db.add(MessageModel(session_id=session_id, role="assistant", content=content_block.text))
                await db.commit()
                
        elif content_block.type == "tool_use":
            await websocket.send_json({"type": "tool_call", "name": content_block.name, "input": content_block.input})
            
            # Execute tool INSIDE the specific container using Docker SDK to guarantee isolation
            container = docker_client.containers.get(container_id)
            if content_block.name == "bash":
                cmd = content_block.input["command"]
                exit_code, output = container.exec_run(["/bin/bash", "-c", cmd])
                await websocket.send_json({"type": "tool_result", "content": output.decode('utf-8')})
            
            # (Implement computer and edit tools similarly by executing their underlying python scripts 
            # inside the container via container.exec_run())