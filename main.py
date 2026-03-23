import asyncio
import urllib.parse
import db
import hashlib
import json
import logging
import secrets
import uvicorn

from contextlib import asynccontextmanager
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi import FastAPI, Header, Request, HTTPException, Response, Depends, status
from http_client import make_dynamic_request
from typing import Annotated, Any

""" lifespan context manager"""


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application startup and shutdown events, handling database and registration."""
    # run the table creation script
    log.info("Start Agent, check DB")
    await db.init_db()
    # check if a registration profile already exists locally
    registration_info = await db.get_registration()
    if not registration_info:
        # trigger external registration if no profile is found
        await register_agent()
    else:
        ongoing_tasks = await db.get_tasks_by_status("ongoing")
        log.info(f"found {len(ongoing_tasks)} tasks")
        for task_record in ongoing_tasks:
            task_id = task_record["id"]
            operation_id = task_record["operation_id"]
            plugin_param = json.loads(task_record["param"])
            # Create and new stop event and save it
            stop_event = asyncio.Event()
            stop_events[task_id] = stop_event

            # start process in background and save it
            task = asyncio.create_task(
                ingestion_worker(task_id, operation_id, plugin_param, stop_event)
            )
            active_tasks[task_id] = task
            log.info(f"Start task {task_id}.")

    # yield control back to fastapi to start serving requests
    yield

    # any teardown logic (e.g., closing global connection pools) would go here
    for event in stop_events.values():
        event.set()

    # AWAIT ALL TASK TO COMPLETE FINALLY BLOCK
    if active_tasks:
        await asyncio.gather(*active_tasks.values(), return_exceptions=True)


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
log = logging.getLogger(__name__)
active_tasks: dict[str, asyncio.Task] = {}
stop_events: dict[str, asyncio.Event] = {}
security = HTTPBearer()
# instantiate the fastapi application with our lifecycle manager
app = FastAPI(lifespan=lifespan)
# load the configuration securely at module level before starting the app
with open("config.json", "r") as config_file:
    app_config = json.load(config_file)

""" SECURITY UTILITY"""


def hash_token(token: str) -> str:
    """Return the SHA-256 hex digest for the provided token.

    Parameters:
    - token: The raw token string to hash.

    Returns:
    - Hexadecimal SHA-256 digest of the token.
    """
    # encode token to bytes and compute sha256 hexdigest
    log.info("Hasing token")
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


async def verify_agent_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
):
    """Verify the incoming Bearer token matches the stored hashed token.

    This dependency is used on protected endpoints to ensure the caller
    is the registered bridge agent.

    Parameters:
    - credentials: Provided by FastAPI's `HTTPBearer` dependency.

    Returns:
    - The raw incoming token string when verification succeeds.

    Raises:
    - HTTPException(401) when no registration exists or the token is invalid.
    """
    # Extract the raw token from the Authorization header
    incoming_token = credentials.credentials
    log.info("Verify token")
    # Retrieve stored registration info (including stored hashed token)
    registration_info = await db.get_registration()
    if not registration_info:
        # No local registration record — deny access
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Agent not registered. Unable to verify credentials.",
        )

    # Hash the incoming token and compare to the stored hash using
    # constant-time comparison to mitigate timing attacks
    incoming_hash = hash_token(incoming_token)
    stored_hash = registration_info["token_auth"]

    if not secrets.compare_digest(incoming_hash, stored_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authorization token.",
            headers={"WWW-Authenticate": "Bearer"},
        )

    # Verified successfully, return the raw token for downstream use
    return incoming_token


""" GULP ENDPOINT"""


async def get_gulp_token() -> str:
    # Attempt to obtain an authentication token from the manager server
    log.info("Start login to manager server.")
    payload = {"user_id": app_config["user_id"], "password": app_config["password"]}
    target_url = f"{app_config['url_server']}/login"
    try:
        response = await make_dynamic_request(
            url=target_url, method="POST", body=payload
        )
        response.raise_for_status()
        # parse JSON body and extract token
        data = response.json()
        token = data.get("data", {}).get("token")
        if token:
            log.info("Login successful: token retrieved.")
            return token
        else:
            log.info("Login returned success but no token present in response.")
            return None
    except Exception as ex:
        log.error(f"Login failed: {ex}")
        # re-raise to surface failure to caller (startup may abort)
        raise ex


async def register_agent() -> None:
    """Register this bridge agent with the manager server and persist credentials.

    This function obtains a manager auth token, generates a local bridge token,
    sends the registration request and saves the resulting registration
    information (including a hashed local bridge token) into the local DB.

    Raises:
    - RuntimeError when the registration request or DB save fails.
    """
    # Build and send registration request
    log.info("Start registering agent with manager server.")
    auth_token = await get_gulp_token()
    target_url = f"{app_config['url_server']}/register_bridge"

    # Generate a new local bridge token (kept secret) and store its hash
    new_bridge_token = secrets.token_urlsafe(32)
    agent_url = f"{app_config["bridge_url"]}:{app_config["bridge_port"]}"
    header = {"token": auth_token, "Accept": "application/json"}

    query_params = {
        "name": app_config["bridge_name"],
        "url": agent_url,
        "bridge_token": new_bridge_token,
    }

    try:
        # Send registration request to manager server
        response = await make_dynamic_request(
            url=target_url, method="POST", headers=header, query_string=query_params
        )
        response.raise_for_status()
        log.info("Agent registered successfully.")
        # extract the server response data
        data = response.json()
        hashed_token = hash_token(new_bridge_token)

        # Persist registration into local DB
        await db.create_registration(
            id=data.get("bridge_id"),
            endpoint=app_config["url_server"],
            status="ready",
            token_auth=hashed_token,
        )
        log.info("Agent saved in DB successfully")
    except Exception as e:
        # Surface the failure so application startup can handle/abort
        message = f"agent registration failed: {str(e)}"
        log.error(message)
        raise RuntimeError(message)


async def ingest_raw_event(
    operation_id: str, plugin_params: dict, event_data: list[dict]
) -> None:
    """Send a batch of raw events to the manager server using multipart form upload.

    Parameters:
    - operation_id: ID of the associated operation on manager side.
    - plugin_params: Parameters object provided by Gulp for plugin execution.
    - event_data: List of event dictionaries to transmit.

    Notes:
    - The function builds a multipart form with two parts: a JSON `payload`
      and a binary `f` part containing the serialized events.
    """
    log.info("Agent Start ingest_raw call")
    # Obtain manager auth token for the request
    auth_token = await get_gulp_token()
    target_url = f"{app_config['url_server']}/ingest_raw"

    header = {"token": auth_token, "Accept": "application/json"}
    query_params = {
        "operation_id": operation_id,
        "plugin": "raw",
        "ws_id": "not_supported",
    }

    try:
        # Build multipart form data with the JSON payload and binary chunk

        # Part 1: JSON payload with plugin parameters
        payload_obj = {"flt": {}, "plugin_params": plugin_params}
        payload_json_bytes = json.dumps(payload_obj).encode("utf-8")

        # Part 2: events serialized as bytes, sent as application/octet-stream
        event_data_to_send = event_data if event_data is not None else []
        chunk_bytes = json.dumps(event_data_to_send, allow_nan=False).encode("utf-8")
        multipart_files = {
            "payload": (None, payload_json_bytes, "application/json"),
            "f": ("chunk", chunk_bytes, "application/octet-stream"),
        }

        # Send request and ensure success status
        response = await make_dynamic_request(
            url=target_url,
            method="POST",
            headers=header,
            query_string=query_params,
            files=multipart_files,
        )
        response.raise_for_status()

        data = response.json()
        log.info(f"response ingest: {data}")
    except Exception as ex:
        # Log the error and continue — ingestion should tolerate transient failures
        log.error(f"Unable to send events: {ex}")


async def set_bridge_task_status(task_id: str, status: str, err: str) -> None:
    log.info(f"set task status on gulp, task_id: {task_id}, status: {status}")

    auth_token = await get_gulp_token()

    target_url = f"{app_config['url_server']}/set_bridge_task_status"
    query_string = {"bridge_task_id": task_id}
    payload = {"status": status, "error": err}
    header = {"token": auth_token, "Accept": "application/json"}
    try:
        response = await make_dynamic_request(
            target_url, "POST", body=payload, query_string=query_string, headers=header
        )
        data = response.json()
        log.info(f"set_bridge_task_status response: {data}")
    except Exception as ex:
        log.error("set status failede: {ex}")
        raise ex


""" INGESTION """


async def ingestion_worker(
    task_id: str, operation_id: str, plugin_params: dict, stop_event: asyncio.Event
) -> None:
    """Background ingestion worker reading journalctl and sending events.

    This coroutine launches `journalctl -f -n 0 -o json` and reads output lines
    continuously. Each JSON log line is annotated with additional fields and
    buffered into batches that are forwarded to `ingest_raw_event`.

    Parameters:
    - task_id: Unique identifier for the ingestion task (bridge_task_id).
    - operation_id: Operation id to attach to each event for manager association.
    - plugin_params: Plugin parameters object that must be forwarded with events.
    - stop_event: asyncio.Event used to signal the worker to stop gracefully.
    """
    log.info(f"Starting ingestion worker for task {task_id}")
    batch_size = app_config.get("batch_size", 50)
    batch_events = []

    try:
        # Start a subprocess that follows the system journal, outputting JSON
        process = await asyncio.create_subprocess_exec(
            "journalctl",
            "-f",
            "-n",
            "0",
            "-o",
            "json",
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        log.info(f"Started journalctl process PID:{process.pid}")

        # Continue until an external stop signal is received
        while not stop_event.is_set():
            try:
                # Read one line from journalctl with a 1 second timeout so that
                # the loop can periodically check the stop_event.
                line = await asyncio.wait_for(process.stdout.readline(), timeout=1.0)

                if not line:
                    # If empty, the subprocess likely exited unexpectedly
                    stderr_data = await process.stderr.read()
                    error_msg = stderr_data.decode("utf-8").strip()
                    log.error(
                        f"journalctl process exited unexpectedly. Stderr: {error_msg}"
                    )
                    break

                # Decode the bytes and parse the JSON record
                event_original = line.decode("utf-8")
                log_entry = json.loads(event_original)

                # Annotate the event with bridge/operation context
                # IMPORTANT THIS SECTION IS MANDATORY FOR ANY BRIDGE
                log_entry["gulp.operation_id"] = operation_id
                log_entry["event.original"] = event_original
                log_entry["gulp.source_id"] = "journalctl"  # identify source type
                log_entry["gulp.context_id"] = app_config[
                    "gulp_context_id"
                ]  # context identifier
                log_entry["@timestamp"] = log_entry.pop("__REALTIME_TIMESTAMP")

                log.info(f"{json.dumps(log_entry,indent=2)}")
                # Append to current batch
                batch_events.append(log_entry)

                # Send the batch when size threshold reached
                if len(batch_events) >= batch_size:
                    log.info(
                        f"Sending batch of {len(batch_events)} events for task {task_id}"
                    )
                    await ingest_raw_event(operation_id, plugin_params, batch_events)
                    batch_events = []

            except asyncio.TimeoutError:
                # No new line in the last second — normal, loop again to check stop_event
                continue
            except json.JSONDecodeError:
                # Skip lines that are not valid JSON
                continue
            except Exception as e:
                # Log and back off briefly in case of transient errors
                log.error(f"Error while reading or sending events: {e}")
                await asyncio.sleep(2)
    finally:
        # Always attempt to terminate the subprocess when exiting this worker
        log.info(f"Stopping journalctl process for task {task_id}...")
        await db.update_task_status(task_id, "stopped")
        stderr_data = await process.stderr.read()
        error_msg = stderr_data.decode("utf-8").strip() or ""
        await set_bridge_task_status(
            task_id, "failed" if error_msg else "stopped", error_msg
        )

        if process.returncode is None:
            log.error(process)
            process.terminate()
            await process.wait()

        # Flush any remaining events before exit
        if batch_events:
            log.info(
                f"Sending last {len(batch_events)} remaining events for task {task_id}"
            )
            await ingest_raw_event(operation_id, batch_events)

    log.info(f"Ingestion worker stopped for task {task_id}")


""" HANDLER AND ENDPOINT"""


async def start_ingestion_handler(
    request: Request,
    token: str = Depends(verify_agent_token),
) -> dict[str, Any]:
    """Handle the start ingestion command by creating a new ongoing task."""
    log.info("/start_ingestion called")
    try:
        # parse the json body from the incoming request
        body = await request.json()
    except Exception:
        # default to an empty dict if body parsing fails or is empty
        body = {}

    # extract the param field safely
    plugin_params = body.get("plugin_params", {})
    try:
        bridge_id = body.get("bridge_id")
        bridge_task_id = body.get("bridge_task_id")
        operation_id = body.get("operation_id")
        if not bridge_task_id or not operation_id or not bridge_id:
            raise ValueError(f"mandatory param not receive, error")
    except Exception as ex:
        log.error(f"error: {ex}")
        raise HTTPException(status_code=400, detail=f"mandatory param error: {ex}")

    log.info(
        f"/bridge_id: {bridge_id}\nbridge_task_id: {bridge_task_id}\noperation_id: {operation_id}"
    )
    if bridge_task_id in active_tasks and not active_tasks[bridge_task_id].done():
        return {"message": "ingestion already running", "task_id": bridge_task_id}

    # check if there is task saved in db, update status in ongoing and retrive params
    existing_task = await db.get_task(bridge_task_id)
    if existing_task:
        # check if params send by gulp is OK
        log.info(f"existring_task: {existing_task}")
        if bridge_id != existing_task.get("bridge_id"):
            raise ValueError(f"task with id: {bridge_task_id} has not same bridge_id")
        if operation_id != existing_task.get("operation_id"):
            raise ValueError(
                f"task with id: {bridge_task_id} alredy exist in a different"
            )
        await db.update_task_status(bridge_task_id, "ongoing")
        plugin_params = json.loads(existing_task.get("param", "{}"))
        log.info(f"task updated")
        log.info(f"plugin_params: {plugin_params}")
    # create a new task record with ongoing status
    else:
        log.info(f"create a new")
        task_id = await db.create_task(
            id=bridge_task_id,
            bridge_id=bridge_id,
            operation_id=operation_id,
            param=json.dumps(plugin_params),
            status="ongoing",
        )

    stop_event = asyncio.Event()
    stop_events[bridge_task_id] = stop_event
    log.info(f"create worker")
    task = asyncio.create_task(
        ingestion_worker(bridge_task_id, operation_id, plugin_params, stop_event)
    )
    active_tasks[bridge_task_id] = task

    # return a structured response to the caller
    return {"message": "ingestion started", "task_id": bridge_task_id}


async def stop_ingestion_handler(
    request: Request,
    token: str = Depends(verify_agent_token),
) -> dict[str, Any]:
    """Handle the stop ingestion command by extracting query strings and updating task status."""
    task_id_str = request.query_params.get("task_id")
    log.info(f"/stop_ingestion called")
    try:
        # parse the json body from the incoming request
        body = await request.json()
    except Exception:
        # default to an empty dict if body parsing fails or is empty
        body = {}

    task_id_str = body.get("bridge_task_id")
    bridge_id = body.get("bridge_id")
    if not task_id_str:
        raise HTTPException(
            status_code=400, detail="valid task_id query parameter is required"
        )
    log.info(f"task_id_str: {task_id_str}\nbridge_id: {bridge_id}")

    existing_task = await db.get_task(task_id_str)
    if not existing_task or existing_task.get("bridge_id") != bridge_id:
        raise HTTPException(status_code=404, detail="task not found")

    await db.update_task_status(task_id_str, "stopped")
    log.info(f"task updated")
    # stop worker
    if task_id_str in stop_events:
        # set stop task signal
        stop_events[task_id_str].set()
        await asyncio.sleep(2)
        # clean memory
        del stop_events[task_id_str]
        if task_id_str in active_tasks:
            del active_tasks[task_id_str]
        log.info(f"task stopped and deleted from active")
    return {"message": "ingestion stopped", "task_id": task_id_str}


async def health_check_handler(token: str = Depends(verify_agent_token)) -> dict:
    """
    Endpoint di health-check per il manager server (Gulp).
    Verifica che il bridge sia attivo, autenticato e operativo.
    """
    try:
        # check db
        bridge = await db.get_registration()
        tasks = await db.get_tasks(bridge_id=bridge["id"])

        return {
            "status": bridge["status"],
            "active_tasks_count": len(active_tasks),
            "tasks": tasks,
            "version": "1.0.0",
        }
    except Exception as ex:
        # Se il DB è rotto o c'è un errore fatale, restituisce 503 Service Unavailable
        log.error(f"failed health check, error: {ex}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Bridge in stato di errore: {str(ex)}",
        )


# dynamically add the api routes using the handler methods as requested
app.add_api_route(
    path="/start_ingestion", endpoint=start_ingestion_handler, methods=["POST"]
)

app.add_api_route(
    path="/stop_ingestion", endpoint=stop_ingestion_handler, methods=["POST"]
)
app.add_api_route(path="/health_check", endpoint=health_check_handler, methods=["POST"])


if __name__ == "__main__":
    log.info("start main")
    raw_url = app_config.get("bridge_url", "http://0.0.0.0")
    parsed_url = urllib.parse.urlparse(raw_url)
    host_address = parsed_url.hostname or "0.0.0.0"
    port_number = app_config.get("bridge_port", 8000)

    uvicorn.run(app, host=host_address, port=port_number, reload=False)
