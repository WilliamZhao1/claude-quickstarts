import uuid
import docker
import asyncio
import json
import tarfile
import io
import socket
import traceback
import httpx
from typing import Dict, Set
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
import anthropic

from manager_app.database import init_db, AsyncSessionLocal, SessionModel, MessageModel

app = FastAPI(title="Computer Use API")
# Initialize Docker client with an extended 120-second timeout for Windows/WSL2
docker_client = docker.from_env(timeout=120)
client = anthropic.AsyncAnthropic()

# --- Connection and Task Management ---
active_websockets: Dict[str, Set[WebSocket]] = {}
active_tasks: Dict[str, asyncio.Task] = {}

async def broadcast(session_id: str, message: dict):
    """Send a message to all connected websockets for a specific session."""
    if session_id in active_websockets:
        dead_sockets = set()
        # Iterate over a copy to safely remove dead connections
        for ws in list(active_websockets[session_id]):
            try:
                await ws.send_json(message)
            except Exception:
                dead_sockets.add(ws)
        
        for ws in dead_sockets:
            if ws in active_websockets[session_id]:
                active_websockets[session_id].remove(ws)

@app.on_event("startup")
async def startup():
    await init_db()

@app.get("/")
async def serve_frontend():
    return FileResponse("frontend/index.html")

async def get_db():
    async with AsyncSessionLocal() as session:
        yield session

class SessionCreateResponse(BaseModel):
    session_id: str
    vnc_port: int

def get_free_port():
    """Finds a guaranteed free port natively on the host machine."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

@app.get("/sessions")
async def list_sessions(db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SessionModel).order_by(SessionModel.created_at.desc()))
    sessions = result.scalars().all()
    return [{
        "session_id": s.id, 
        "vnc_port": s.vnc_port, 
        "created_at": s.created_at.isoformat() if s.created_at else "", 
        "status": s.status
    } for s in sessions]

@app.get("/sessions/{session_id}/messages")
async def get_session_messages(session_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(MessageModel).filter(MessageModel.session_id == session_id).order_by(MessageModel.id))
    messages = result.scalars().all()
    return [{"role": m.role, "content": json.loads(m.content)} for m in messages]

@app.get("/sessions/{session_id}/health")
async def check_session_health(session_id: str, db: AsyncSession = Depends(get_db)):
    result = await db.execute(select(SessionModel).filter(SessionModel.id == session_id))
    session_data = result.scalars().first()
    if not session_data:
        raise HTTPException(status_code=404, detail="Session not found")
    
    try:
        async with httpx.AsyncClient() as ac:
            response = await ac.get(f"http://127.0.0.1:{session_data.vnc_port}/vnc.html", timeout=1.0)
            if response.status_code == 200:
                return {"ready": True}
    except Exception:
        pass
    return {"ready": False}

@app.post("/sessions", response_model=SessionCreateResponse)
async def create_session(db: AsyncSession = Depends(get_db)):
    try:
        session_id = str(uuid.uuid4())
        host_port = get_free_port()
        
        container = await asyncio.to_thread(
            docker_client.containers.run,
            "computer-use-demo:local",
            detach=True,
            shm_size="2g",
            ports={
                '6080/tcp': host_port
            },
            environment={"WIDTH": "1024", "HEIGHT": "768"}
        )
            
        new_session = SessionModel(
            id=session_id, 
            container_id=container.id, 
            vnc_port=host_port
        )
        db.add(new_session)
        await db.commit()
        
        return SessionCreateResponse(session_id=session_id, vnc_port=host_port)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/sessions/{session_id}/stream")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    
    # Register the active websocket connection
    if session_id not in active_websockets:
        active_websockets[session_id] = set()
    active_websockets[session_id].add(websocket)
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(SessionModel).filter(SessionModel.id == session_id))
        session_data = result.scalars().first()
        if not session_data:
            active_websockets[session_id].remove(websocket)
            await websocket.close(code=1008)
            return

    try:
        while True:
            data = await websocket.receive_json()
            user_msg = data.get("prompt")
            
            if user_msg:
                content_list = [{"type": "text", "text": user_msg}]
                async with AsyncSessionLocal() as db:
                    db.add(MessageModel(session_id=session_id, role="user", content=json.dumps(content_list)))
                    await db.commit()
                
                await broadcast(session_id, {"type": "progress", "content": "Thinking..."})
                
                # Check if an agent is already running for this session
                if session_id in active_tasks and not active_tasks[session_id].done():
                    await broadcast(session_id, {"type": "progress", "content": "Agent is already processing a task..."})
                else:
                    # Fire and forget the background task
                    active_tasks[session_id] = asyncio.create_task(
                        run_agent_loop(session_id, session_data.container_id)
                    )
            
    except WebSocketDisconnect:
        # Gracefully handle disconnect without killing the background loop
        if session_id in active_websockets and websocket in active_websockets[session_id]:
            active_websockets[session_id].remove(websocket)

def _exec_tool_in_container(container_id: str, tool_name: str, tool_input: dict):
    container = docker_client.containers.get(container_id)
    
    if tool_name == "bash":
        module, cls = "bash", "BashTool20250124"
    elif tool_name == "computer":
        module, cls = "computer", "ComputerTool20251124"
    elif tool_name == "str_replace_based_edit_tool":
        module, cls = "edit", "EditTool20250728"
    else:
        raise ValueError(f"Unknown tool {tool_name}")

    script = f"""
import sys
sys.path.insert(0, '/home/computeruse')

import asyncio
import json
import base64
from computer_use_demo.tools.{module} import {cls}

async def main():
    tool = {cls}()
    try:
        res = await tool(**{json.dumps(tool_input)})
        print(json.dumps({{
            "output": getattr(res, "output", None), 
            "error": getattr(res, "error", None), 
            "base64_image": getattr(res, "base64_image", None)
        }}))
    except Exception as e:
        print(json.dumps({{"error": str(e)}}))

asyncio.run(main())
"""
    tar_stream = io.BytesIO()
    with tarfile.open(fileobj=tar_stream, mode='w') as tar:
        script_bytes = script.encode('utf-8')
        tarinfo = tarfile.TarInfo(name='run_tool.py')
        tarinfo.size = len(script_bytes)
        tar.addfile(tarinfo, io.BytesIO(script_bytes))
    tar_stream.seek(0)
    
    container.put_archive('/tmp/', tar_stream)
    
    return container.exec_run(
        cmd=["python", "/tmp/run_tool.py"],
        user="computeruse",
        workdir="/home/computeruse"
    )

async def run_agent_loop(session_id: str, container_id: str):
    system_prompt = "You are a computer use agent. You have access to a sandboxed Ubuntu environment."
    
    try:
        async with AsyncSessionLocal() as db:
            result = await db.execute(select(MessageModel).filter(MessageModel.session_id == session_id).order_by(MessageModel.id))
            past_messages = result.scalars().all()
            
        messages = [{"role": msg.role, "content": json.loads(msg.content)} for msg in past_messages]

        while True:
            response = await client.beta.messages.create(
                model="claude-opus-4-8",
                max_tokens=1024,
                betas=["computer-use-2025-11-24"], 
                system=system_prompt,
                messages=messages,
                tools=[
                    {"type": "computer_20251124", "name": "computer", "display_width_px": 1024, "display_height_px": 768, "display_number": 1},
                    {"type": "bash_20250124", "name": "bash"},
                    {"type": "text_editor_20250728", "name": "str_replace_based_edit_tool"}
                ]
            )

            assistant_content = response.content
            assistant_content_dicts = [b.model_dump() for b in assistant_content]
            messages.append({"role": "assistant", "content": assistant_content_dicts})

            async with AsyncSessionLocal() as db:
                db.add(MessageModel(session_id=session_id, role="assistant", content=json.dumps(assistant_content_dicts)))
                await db.commit()

            tool_results = []
            has_tool_use = False

            for content_block in assistant_content:
                if content_block.type == "text":
                    await broadcast(session_id, {"type": "message", "role": "assistant", "content": content_block.text})
                        
                elif content_block.type == "tool_use":
                    has_tool_use = True
                    tool_name = content_block.name
                    tool_input = content_block.input
                    tool_id = content_block.id
                    
                    await broadcast(session_id, {"type": "tool_call", "name": tool_name, "input": tool_input})
                    
                    exit_code, output = await asyncio.to_thread(_exec_tool_in_container, container_id, tool_name, tool_input)
                    
                    try:
                        if exit_code != 0:
                            raise ValueError(output.decode('utf-8'))
                            
                        result_dict = json.loads(output.decode('utf-8').strip())
                        tool_res_content = []
                        
                        if result_dict.get('output'):
                            tool_res_content.append({"type": "text", "text": result_dict['output']})
                        if result_dict.get('error'):
                            tool_res_content.append({"type": "text", "text": f"Error: {result_dict['error']}"})
                        if result_dict.get('base64_image'):
                            tool_res_content.append({
                                "type": "image", 
                                "source": {"type": "base64", "media_type": "image/png", "data": result_dict['base64_image']}
                            })
                            await broadcast(session_id, {"type": "tool_result", "content": "Screenshot captured."})
                        else:
                            await broadcast(session_id, {"type": "tool_result", "content": result_dict.get('output') or "Completed."})
                            
                        if not tool_res_content:
                            tool_res_content.append({"type": "text", "text": "Command executed successfully with no output."})
                            
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": tool_res_content
                        })

                    except Exception as parse_err:
                        err_str = output.decode('utf-8') if output else str(parse_err)
                        await broadcast(session_id, {"type": "tool_result", "content": f"Failed execution: {err_str}"})
                        tool_results.append({"type": "tool_result", "tool_use_id": tool_id, "content": [{"type": "text", "text": err_str}]})

            if has_tool_use:
                messages.append({"role": "user", "content": tool_results})
                async with AsyncSessionLocal() as db:
                    db.add(MessageModel(session_id=session_id, role="user", content=json.dumps(tool_results)))
                    await db.commit()
            else:
                break

    except Exception as e:
        await broadcast(session_id, {"type": "progress", "content": f"API Error: {str(e)}"})
    finally:
        # Clean up the task reference when the agent loop completes
        if session_id in active_tasks:
            active_tasks.pop(session_id, None)