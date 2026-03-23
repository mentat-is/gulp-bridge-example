import aiosqlite
from typing import Optional, List, Dict, Any

# define the sqlite database file name
db_path = "agent.db"


async def init_db() -> None:
    """Initialize the sqlite database and create necessary tables if they do not exist."""
    # connect to the sqlite database asynchronously
    async with aiosqlite.connect(db_path) as db:
        # create the registration table to store agent details
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS registration (
                id TEXT PRIMARY KEY,
                endpoint TEXT,
                status TEXT,
                token_auth TEXT
            )
        """
        )

        # create the tasks table to track ingestion processes
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                id TEXT PRIMARY KEY,
                bridge_id TEXT,
                param TEXT,
                operation_id TEXT,
                status TEXT
            )
        """
        )
        # commit the schema creation to disk
        await db.commit()


async def create_registration(
    id: str, endpoint: str, status: str, token_auth: str
) -> None:
    """Insert a new registration record into the database."""
    # open database connection
    async with aiosqlite.connect(db_path) as db:
        # execute parameterized query to prevent sql injection
        await db.execute(
            "INSERT INTO registration (id, endpoint, status, token_auth) VALUES (?,?, ?, ?)",
            (id, endpoint, status, token_auth),
        )
        # persist the changes
        await db.commit()


async def get_registration() -> Optional[Dict[str, Any]]:
    """Retrieve the first registration record from the database, if it exists."""
    # open database connection
    async with aiosqlite.connect(db_path) as db:
        # configure row factory to return dictionary-like rows
        db.row_factory = aiosqlite.Row
        # fetch the first registration record
        async with db.execute("SELECT * FROM registration LIMIT 1") as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def update_registration_status(reg_id: int, status: str) -> None:
    """Update the status of an existing registration record."""
    # open database connection
    async with aiosqlite.connect(db_path) as db:
        # update the status field for the specific id
        await db.execute(
            "UPDATE registration SET status = ? WHERE id = ?", (status, reg_id)
        )
        # commit the update
        await db.commit()


async def delete_registration(reg_id: int) -> None:
    """Delete a registration record by its id."""
    # open database connection
    async with aiosqlite.connect(db_path) as db:
        # delete the record matching the provided id
        await db.execute("DELETE FROM registration WHERE id = ?", (reg_id,))
        # commit the deletion
        await db.commit()


async def create_task(
    id: str, bridge_id: str, operation_id: str, param: str, status: str
) -> int:
    """Insert a new task into the database and return its generated id."""
    # open database connection
    async with aiosqlite.connect(db_path) as db:
        # insert the task data safely
        cursor = await db.execute(
            "INSERT INTO tasks (id, bridge_id, operation_id, param, status) VALUES (?, ?, ?, ?, ?)",
            (id, bridge_id, operation_id, param, status),
        )
        # commit to generate the id
        await db.commit()
        # return the id
        return cursor.lastrowid


async def get_tasks_by_status(status: str) -> List[Dict[str, Any]]:
    """Retrieve all tasks matching a specific status."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tasks WHERE status = ?", (status,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_task(task_id: str) -> Optional[Dict[str, Any]]:
    """Retrieve a specific task by its id."""
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def get_tasks(bridge_id: str) -> Optional[list[dict]]:
    async with aiosqlite.connect(db_path) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM tasks WHERE bridge_id = ?", (bridge_id,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows] if rows else []


async def update_task_status(task_id: str, status: str) -> None:
    """Update the status of a specific task."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        await db.commit()


async def delete_task(task_id: str) -> None:
    """Delete a task record from the database."""
    async with aiosqlite.connect(db_path) as db:
        await db.execute("DELETE FROM tasks WHERE id = ?", (task_id,))
        await db.commit()
