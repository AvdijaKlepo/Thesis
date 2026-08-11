from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from pydantic import BaseModel
import traceback
import sys
import httpx

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

app = FastAPI()


app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)


class InstanceRequest(BaseModel):
    count: int

class OhaRequest(BaseModel):
    concurrency:int
    duration:int

class AlgorithmSwitchRequest(BaseModel):
    algorithm:str

class BreakRecoverInstance(BaseModel):
    name:str

process_registry = []

@app.post("/start-oha")
async def start_oha(req: OhaRequest):
    if (req.concurrency<=0 or req.concurrency>50) and (req.duration<=0 or req.duration>50):
        raise HTTPException(status_code=400, detail="Keep concurrency and duration between 1 and 50.")
    
    bash_dir = r"C:/Users/Avdija"
    concurrency = req.concurrency
    duration = req.duration

    bash_string = f"oha -z {duration}s -c {concurrency} http://127.0.0.1:7879"


    spawn_window_cmd = f'start cmd /k "{bash_string}"'

    commands = []

    process = commands.append(asyncio.create_subprocess_shell(
        spawn_window_cmd
    ))

    

    try:
      processes = await asyncio.gather(*commands)
      process_registry.extend(processes)
      return {
          "message": f"Successfully started an oha test with a concurrency of {concurrency} and a duration of {duration}"
      }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

    


@app.post("/start-workers")
async def start_workers(req: InstanceRequest):
    if req.count <= 0 or req.count > 50:
      raise HTTPException(status_code=400, detail="Count must be between 1 and 50 workers.")
    
    rust_dir = r"C:/Users/Avdija/Documents/WebServer/server"   
    base_port = 8081
    base_id = 1
    commands = []
  

 

    for i in range(req.count):
        current_port = str(base_port + i)
        current_id = str(base_id + i)
        cmd_string = f"cargo run --bin backend -- {current_port} {current_id}"
        spawn_windows_cmd = f'start "" /D "{rust_dir}" cmd /k "{cmd_string}"'

        commands.append(asyncio.create_subprocess_shell(
           spawn_windows_cmd
        ))

    try:
        processes = await asyncio.gather(*commands)
        process_registry.extend(processes)

        assigned_ports = [base_port + i for i in range(req.count)]
        return {
            "message": f"Successfully started {req.count} Rust backend instance(s)",
            "ports": assigned_ports
        }
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/switch-algorithm")
async def switch_algorithm(algorithm:AlgorithmSwitchRequest):
    async with httpx.AsyncClient() as client:
        resp = await client.post(
            "http://127.0.0.1:7880/admin/algorithm",
            content=algorithm.algorithm,
            timeout=2.0,
            headers={"Content-Type": "text/plain"}
        )
    return {"status": resp.status_code, "message": resp.text}


@app.post("/break_backend")
async def break_backend(name:BreakRecoverInstance):
    async with httpx.AsyncClient() as client:
        resp =await client.post(f"http://127.0.0.1:8474/proxies/{name.name}", json={"enabled": False})
    return {"status":resp.status_code, "body":resp.text}


@app.post("/recover_backend")
async def recover_backend(name:BreakRecoverInstance):
    async with httpx.AsyncClient() as client:
        resp =await client.post(f"http://127.0.0.1:8474/proxies/{name.name}", json={"enabled": True})
    return {"status":resp.status_code, "body":resp.text}
        


  




