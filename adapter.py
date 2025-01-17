import os
import logging
import time
import ray

from flask import Flask, request, jsonify
import sqlparse
import pymongo
import psycopg2

##############################################################################
# Configuration & Logging
##############################################################################
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s in %(module)s: %(message)s'
)
logger = logging.getLogger(__name__)

##############################################################################
# Environment Variables
##############################################################################
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://mongodb:27017/")
DB_NAME = os.environ.get("DB_NAME", "testdb")
POSTGRES_URI = os.environ.get("POSTGRES_URI", "postgresql://user:password@postgres:5432/testdb")

##############################################################################
# Initialize Ray (Local Mode)
##############################################################################
# For a real cluster, you might connect differently:
# ray.init(address="auto")  # connect to a Ray cluster
ray.init(ignore_reinit_error=True, include_dashboard=False)

##############################################################################
# DBActor Class (Ray)
##############################################################################
@ray.remote
class DBActor:
    """
    A Ray Actor that holds connections to Mongo & Postgres internally.
    This avoids having to pickle the DB connections in remote tasks.
    """
    def __init__(self, mongo_uri, db_name, postgres_uri):
        self.mongo_client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
        self.mongo_db = self.mongo_client[db_name]

        self.postgres_conn = psycopg2.connect(postgres_uri)
        self.postgres_conn.autocommit = True
        self.postgres_cursor = self.postgres_conn.cursor()

    def run_mongo_select(self, sql_query: str):
        """
        Parse tokens, execute on Mongo.
        Returns (results_list, elapsed_time).
        """
        tokens = self._parse_tokens(sql_query)
        start = time.perf_counter()
        results = self._execute_select_mongo(tokens)
        elapsed = time.perf_counter() - start
        return results, elapsed

    def run_postgres_select(self, sql_query: str):
        """
        Run native Postgres. 
        Returns (results_list, elapsed_time).
        """
        start = time.perf_counter()
        self.postgres_cursor.execute(sql_query)
        if self.postgres_cursor.description:
            rows = self.postgres_cursor.fetchall()
        else:
            rows = []
        elapsed = time.perf_counter() - start
        return rows, elapsed

    ##########################################################################
    # Helpers
    ##########################################################################
    def _parse_tokens(self, sql_query):
        parsed_statements = sqlparse.parse(sql_query)
        if not parsed_statements:
            raise ValueError("Unable to parse the SQL query")
        statement = parsed_statements[0]
        tokens = [t for t in statement.tokens if not t.is_whitespace]
        if not tokens:
            raise ValueError("Invalid SQL query (no tokens).")
        return tokens

    def _execute_select_mongo(self, tokens):
        """
        Simplified SELECT ... FROM ...
        Allows optional WHERE, LIMIT, OFFSET.
        """
        select_idx = None
        from_idx = None
        where_idx = None
        limit_val = None
        offset_val = None

        for i, t in enumerate(tokens):
            val = t.value.upper()
            if val == "SELECT":
                select_idx = i
            elif val == "FROM":
                from_idx = i
            elif val == "WHERE":
                where_idx = i
            elif val == "LIMIT":
                try:
                    limit_val = int(tokens[i+1].value)
                except (IndexError, ValueError):
                    pass
            elif val == "OFFSET":
                try:
                    offset_val = int(tokens[i+1].value)
                except (IndexError, ValueError):
                    pass

        if from_idx is None:
            raise ValueError("No 'FROM' clause found")

        # Identify collection name
        fields_token = tokens[select_idx + 1] if select_idx is not None else None
        table_token = tokens[from_idx + 1] if (from_idx + 1) < len(tokens) else None
        if not table_token:
            raise ValueError("No collection specified")

        collection_name = table_token.value.strip()
        projection = self._build_projection(fields_token)
        filter_doc = {}

        # Parse WHERE if present
        if where_idx is not None:
            where_tokens = tokens[where_idx + 1:]
            filter_doc = self._parse_where_clause(where_tokens)

        cursor = self.mongo_db[collection_name].find(filter_doc, projection)
        if offset_val is not None:
            cursor = cursor.skip(offset_val)
        if limit_val is not None:
            cursor = cursor.limit(limit_val)

        return list(cursor)

    def _build_projection(self, fields_token):
        if not fields_token or fields_token.value.strip() == "*":
            return {"_id": 0}

        raw_cols = fields_token.value.replace("(", "").replace(")", "")
        parts = [p.strip() for p in raw_cols.split(",")]
        parts = [p for p in parts if p.upper() != "SELECT"]
        proj = {col: 1 for col in parts}
        proj["_id"] = 0
        return proj

    def _parse_where_clause(self, tokens):
        where_str = " ".join(t.value for t in tokens)
        conditions = [c.strip() for c in where_str.split("AND")]

        filter_doc = {}
        for cond in conditions:
            parts = cond.split()
            if len(parts) < 3:
                continue
            field, op, val = parts[0], parts[1], " ".join(parts[2:])
            val = val.strip("'").strip('"')

            # Attempt numeric cast
            cast_val = None
            try:
                if "." in val:
                    cast_val = float(val)
                else:
                    cast_val = int(val)
            except ValueError:
                pass

            final_val = cast_val if cast_val is not None else val
            if op == "=":
                filter_doc[field] = final_val
            elif op == ">":
                filter_doc[field] = {"$gt": final_val}
            elif op == "<":
                filter_doc[field] = {"$lt": final_val}
        return filter_doc

##############################################################################
# Flask Setup (Non-Ray Endpoints)
##############################################################################
app = Flask(__name__)

# We can still keep global connections for the normal endpoints if you wish,
# or skip them. Here, we create them for /health & /inspect as before:
mongo_client_global = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
mongo_db_global = mongo_client_global[DB_NAME]

try:
    postgres_conn_global = psycopg2.connect(POSTGRES_URI)
    postgres_conn_global.autocommit = True
    postgres_cursor_global = postgres_conn_global.cursor()
except Exception as e:
    logger.error(f"Error connecting to Postgres globally: {e}")
    raise

##############################################################################
# Create a Single DBActor Instance
##############################################################################
# We'll create one actor in global scope so we can reuse it for queries
db_actor = DBActor.remote(MONGO_URI, DB_NAME, POSTGRES_URI)

##############################################################################
# Health Endpoint
##############################################################################
@app.route("/health", methods=["GET"])
def health():
    """
    Quick check using global connections
    """
    try:
        mongo_client_global.admin.command('ping')
        postgres_cursor_global.execute("SELECT 1;")
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.error(f"/health error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

##############################################################################
# Inspect Endpoint
##############################################################################
@app.route("/inspect", methods=["GET"])
def inspect():
    """
    Simple demonstration using global Mongo.
    """
    try:
        collections = mongo_db_global.list_collection_names()
        data = {}
        for coll in collections:
            docs = list(mongo_db_global[coll].find({}, {"_id": 0}).limit(50))
            data[coll] = docs
        return jsonify({"collections": collections, "data": data}), 200
    except Exception as e:
        logger.error(f"/inspect error: {e}")
        return jsonify({"error": str(e)}), 500

##############################################################################
# SpeedTest Endpoint - Uses DBActor
##############################################################################
@app.route("/speedtest", methods=["GET"])
def speedtest():
    """
    Compare performance of:
      1) Mongo-in-the-middle (via DBActor run_mongo_select)
      2) Native Postgres (via DBActor run_postgres_select)
    Query provided via ?query=SELECT%20... 
    """
    sql_query = request.args.get("query", "")
    if not sql_query:
        return jsonify({"error": "No SQL query provided"}), 400

    # We'll do it in parallel with Ray's .remote calls
    start_all = time.perf_counter()

    # Launch Ray tasks in parallel
    mongo_future = db_actor.run_mongo_select.remote(sql_query)
    pg_future = db_actor.run_postgres_select.remote(sql_query)

    # Gather results
    mongo_results, mongo_time = ray.get(mongo_future)
    pg_results, pg_time = ray.get(pg_future)

    total_time = time.perf_counter() - start_all

    return jsonify({
        "total_parallel_time": total_time,
        "mongo_in_the_middle_time": mongo_time,
        "mongo_in_the_middle_count": len(mongo_results),
        "native_postgres_time": pg_time,
        "native_postgres_count": len(pg_results)
    })

##############################################################################
# Main
##############################################################################
if __name__ == "__main__":
    # For real production, use gunicorn/uvicorn + multiple workers, e.g.:
    # gunicorn -w 4 -b 0.0.0.0:5000 adapter:app
    app.run(host="0.0.0.0", port=5000, debug=True)

"""
curl "http://localhost:5000/speedtest?query=SELECT%20*%20FROM%20users"
{
  "mongo_in_the_middle_count": 500000,
  "mongo_in_the_middle_time": 0.423658875000001,
  "native_postgres_count": 500000,
  "native_postgres_time": 0.1393247500000001,
  "total_parallel_time": 1.0813827919999994
}
"""