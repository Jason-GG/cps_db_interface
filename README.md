# db-interface

A dynamic db-based database interface for creating and interacting with models on the fly.

This module allows you to:
- Dynamically define db models via schema dictionaries
- Create tables in a PostgreSQL (or other SQL) database
- Query and update data with a lightweight handler class

---

## 📦 Installation

Assuming you've packaged and uploaded it to a private repository (e.g. AWS CodeArtifact):

```bash
pip install db-interface --extra-index-url https://<your-codeartifact-url>/simple/
```
Or locally:

bash
Copy
Edit
pip install .
🧩 Requirements
Python 3.7+

db

psycopg2 (for PostgreSQL) or pymysql (for MySQL)

Install dependencies:

bash
Copy
Edit
pip install sqlalchemy psycopg2
📁 Directory Structure
```plaintext
db_interface/
├── __init__.py
├── db_interface.py      # Core model-building and DB interaction logic
├── tool.py              # Utilities (e.g., logging, reflection)
```
🚀 Example Usage
```aiignore
from db_pg_interface import create_model_class, create_data_table_class
import datetime

# Define table schema

schema = [
    {"name": "id", "type": "Integer", "primary_key": True, "autoincrement": True},
    {"name": "name", "type": "String", "length": 50, "nullable": True},
    {"name": "created_at", "type": "DateTime", "default": None}
]



# Create a db model class
ExampleModel = create_model_class("example_table", schema)

# Add a custom classmethod (optional)
def custom_get_attr(cls):
    return getattr(cls, 'name', None)

ExampleModel.get_attr = classmethod(custom_get_attr)

# Database configuration

dbDict = {
    "MYSQL_DB": "aqt_info_db",
    "MYSQL_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
    "MYSQL_USER": "xxxxxxxxxxxxxxxx",
    "MYSQL_PASSWORD": "xxxxxxxxxxxxxxx",
    "MYSQL_CONNECTOR": "psycopg2"  # Use "pymysql" for MySQL
}



# Create a handler for CRUD operations
ExampleModelHandler = create_data_table_class("ExampleModel", ExampleModel)
handler_instance = ExampleModelHandler(dbDict)

# Retrieve data
user = handler_instance.data_get({"name": "Jason1"})
print(user)
```

🔧 Configuration
MYSQL_CONNECTOR: "psycopg2" for PostgreSQL, or "pymysql" for MySQL

Ensure the user in dbDict has privileges to read/write on the specified DB

🛡️ Security Note
Avoid hardcoding database credentials in production code. Use environment variables or secrets management tools.

📄 License
MIT License

👤 Author
Jason Jian (sjian)

vbnet
Copy
Edit

Let me know if you also want a `pyproject.toml` or `setup.py` to go with it.