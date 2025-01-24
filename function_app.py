import azure.functions as func
import psycopg2
import logging
import os
import json
from psycopg2 import pool
from dotenv import load_dotenv

# Load environment variables from a .env file (for local development)
load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

# Database connection details loaded from environment variables
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "port": os.getenv("DB_PORT", "5432")  # Default PostgreSQL port
}

# Set up database connection pooling
db_pool = pool.SimpleConnectionPool(
    minconn=1,
    maxconn=10,
    **DB_CONFIG
)

def get_db_connection():
    """Fetch a connection from the connection pool."""
    try:
        return db_pool.getconn()
    except Exception as e:
        logging.error(f"Error getting connection from pool: {e}")
        raise

@app.route(route="item", methods=["POST"])
def create_item(req: func.HttpRequest) -> func.HttpResponse:
    """Create a new item."""
    try:
        req_body = req.get_json()
        name = req_body.get("name")
        description = req_body.get("description")

        if not name or not description:
            return func.HttpResponse("Missing 'name' or 'description' in the request body.", status_code=400)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO items (name, description) VALUES (%s, %s) RETURNING id;",
            (name, description)
        )
        item_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)
        

        return func.HttpResponse(json.dumps({"id": item_id, "message": "Item created successfully"}), status_code=201)
    except psycopg2.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        return func.HttpResponse("Database error occurred.", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)

@app.route(route="item/{id}", methods=["GET"])
def read_item(req: func.HttpRequest) -> func.HttpResponse:
    """Read an item by ID."""
    try:
        item_id = req.route_params.get("id")
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT id, name, description FROM items WHERE id = %s;", (item_id,))
        item = cursor.fetchone()
        cursor.close()
        db_pool.putconn(conn)

        if item:
            return func.HttpResponse(json.dumps({"id": item[0], "name": item[1], "description": item[2]}), status_code=200)
        else:
            return func.HttpResponse("Item not found.", status_code=404)
    except psycopg2.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        return func.HttpResponse("Database error occurred.", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)

@app.route(route="item/{id}", methods=["PUT"])
def update_item(req: func.HttpRequest) -> func.HttpResponse:
    """Update an item by ID."""
    try:
        item_id = req.route_params.get("id")
        req_body = req.get_json()
        name = req_body.get("name")
        description = req_body.get("description")

        if not name or not description:
            return func.HttpResponse("Missing 'name' or 'description' in the request body.", status_code=400)

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "UPDATE items SET name = %s, description = %s WHERE id = %s;",
            (name, description, item_id)
        )
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)

        return func.HttpResponse(f"Item {item_id} updated successfully.", status_code=200)
    except psycopg2.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        return func.HttpResponse("Database error occurred.", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)

@app.route(route="item/{id}", methods=["DELETE"])
def delete_item(req: func.HttpRequest) -> func.HttpResponse:
    """Delete an item by ID."""
    try:
        item_id = req.route_params.get("id")

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM items WHERE id = %s;", (item_id,))
        conn.commit()
        cursor.close()
        db_pool.putconn(conn)

        return func.HttpResponse(f"Item {item_id} deleted successfully.", status_code=200)
    except psycopg2.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        return func.HttpResponse("Database error occurred.", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)
    

    
@app.route(route="items", methods=["GET"])
def get_all_items(req: func.HttpRequest) -> func.HttpResponse:
    """Retrieve all items."""
    try:
        # Get a database connection from the pool
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Execute a query to fetch all items
        cursor.execute("SELECT id, name, description FROM items;")
        items = cursor.fetchall()
        
        # Close the connection and release it back to the pool
        cursor.close()
        db_pool.putconn(conn)

        # Check if there are any items, then return them
        if items:
            # Return the items in JSON format
            result = [{"id": item[0], "name": item[1], "description": item[2]} for item in items]
            return func.HttpResponse(json.dumps(result), status_code=200, mimetype="application/json")
        else:
            return func.HttpResponse("No items found.", status_code=404)
    except psycopg2.DatabaseError as db_err:
        logging.error(f"Database error: {db_err}")
        return func.HttpResponse("Database error occurred.", status_code=500)
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse("An unexpected error occurred.", status_code=500)
