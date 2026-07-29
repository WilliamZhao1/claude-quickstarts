import uuid
import docker
import asyncio
import json
import tarfile
import io
import traceback
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Depends, HTTPException
from fastapi.responses import FileResponse
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.future import select
from pydantic import BaseModel
import anthropic

from manager_app.database import init_db, AsyncSessionLocal, SessionModel, MessageModel

app = FastAPI(title="Computer Use API")
docker_client = docker.from_env()
client = anthropic.AsyncAnthropic()

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

@app.post("/sessions", response_model=SessionCreateResponse)
async def create_session(db: AsyncSession = Depends(get_db)):
    try:
        session_id = str(uuid.uuid4())
        
        container = await asyncio.to_thread(
            docker_client.containers.run,
            "computer-use-demo:local",
            detach=True,
            shm_size="2g",
            ports={
                '6080/tcp': None,
                '5900/tcp': None,
                '8501/tcp': None,
                '8080/tcp': None
            },
            environment={"WIDTH": "1024", "HEIGHT": "768"}
        )
        
        await asyncio.sleep(3) 
        await asyncio.to_thread(container.reload)
        
        if container.status != "running":
            error_logs = await asyncio.to_thread(container.logs)
            await asyncio.to_thread(container.remove, force=True)
            raise Exception(f"Container crashed. Logs: {error_logs.decode('utf-8')}")
            
        # Safely extract port binding from container attributes
        network_settings = container.attrs.get('NetworkSettings', {})
        ports_dict = network_settings.get('Ports', {})
        vnc_bindings = ports_dict.get('6080/tcp')
        
        if not vnc_bindings or not vnc_bindings[0].get('HostPort'):
            raise Exception("Failed to retrieve host port mapping for noVNC (6080/tcp).")
            
        vnc_port = int(vnc_bindings[0]['HostPort'])
            
        new_session = SessionModel(
            id=session_id, 
            container_id=container.id, 
            vnc_port=vnc_port
        )
        db.add(new_session)
        await db.commit()
        
        return SessionCreateResponse(session_id=session_id, vnc_port=vnc_port)
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.websocket("/sessions/{session_id}/stream")
async def websocket_endpoint(websocket: WebSocket, session_id: str):
    await websocket.accept()
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(SessionModel).filter(SessionModel.id == session_id))
        session_data = result.scalars().first()
        if not session_data:
            await websocket.close(code=1008)
            return

    try:
        while True:
            data = await websocket.receive_json()
            user_msg = data.get("prompt")
            
            content_list = [{"type": "text", "text": user_msg}]
            async with AsyncSessionLocal() as db:
                db.add(MessageModel(session_id=session_id, role="user", content=json.dumps(content_list)))
                await db.commit()
            
            await websocket.send_json({"type": "progress", "content": "Thinking..."})
            await run_agent_loop(websocket, session_id, session_data.container_id, user_msg)
            
    except WebSocketDisconnect:
        pass

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

    # Explicitly inject /home/computeruse into sys.path so the module can be found
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

async def run_agent_loop(websocket: WebSocket, session_id: str, container_id: str, prompt: str):
    system_prompt = "You are a computer use agent. You have access to a sandboxed Ubuntu environment."
    
    async with AsyncSessionLocal() as db:
        result = await db.execute(select(MessageModel).filter(MessageModel.session_id == session_id).order_by(MessageModel.id))
        past_messages = result.scalars().all()
        
    messages = [{"role": msg.role, "content": json.loads(msg.content)} for msg in past_messages]

    try:
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
                    await websocket.send_json({"type": "message", "role": "assistant", "content": content_block.text})
                        
                elif content_block.type == "tool_use":
                    has_tool_use = True
                    tool_name = content_block.name
                    tool_input = content_block.input
                    tool_id = content_block.id
                    
                    await websocket.send_json({"type": "tool_call", "name": tool_name, "input": tool_input})
                    
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
                            await websocket.send_json({"type": "tool_result", "content": "Screenshot captured."})
                        else:
                            await websocket.send_json({"type": "tool_result", "content": result_dict.get('output') or "Completed."})
                            
                        if not tool_res_content:
                            tool_res_content.append({"type": "text", "text": "Command executed successfully with no output."})
                            
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": tool_id,
                            "content": tool_res_content
                        })

                    except Exception as parse_err:
                        err_str = output.decode('utf-8') if output else str(parse_err)
                        await websocket.send_json({"type": "tool_result", "content": f"Failed execution: {err_str}"})
                        tool_results.append({"type": "tool_result", "tool_use_id": tool_id, "content": [{"type": "text", "text": err_str}]})

            if has_tool_use:
                messages.append({"role": "user", "content": tool_results})
                async with AsyncSessionLocal() as db:
                    db.add(MessageModel(session_id=session_id, role="user", content=json.dumps(tool_results)))
                    await db.commit()
            else:
                break

    except Exception as e:
        await websocket.send_json({"type": "progress", "content": f"API Error: {str(e)}"})