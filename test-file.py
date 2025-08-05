from cps_db_pg_interface import create_model_class, createTables, logger
from cps_db_pg_interface  import create_data_table_class
import datetime

def custom_get_attr(cls):
    # Custom function to retrieve data from the database
    # This is just a placeholder for demonstration purposes
    return getattr(cls, 'name', None)

def custom_get_insert_table_info(cls, args):
    return cls(**{k: v for k, v in args.items() if k != "id"})

def get_test_table_data():
    # Example usage of create_model_class
    tablename = "example_table"
    schema = [
        {"name": "id", "type": "Integer", "primary_key": True, "autoincrement": True},
        {"name": "name", "type": "String", "length": 50, "nullable": True},
        {"name": "created_at", "type": "DateTime", "default": None}
    ]

    ExampleModel = create_model_class(tablename, schema)
    ExampleModel.get_attr = classmethod(custom_get_attr)
    logger.info(ExampleModel.__table__)
    logger.info(ExampleModel.__dict__)
    # createTables(
    #     {
    #         "MYSQL_DB": "aqt_info_db",
    #         "MYSQL_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
    #         "MYSQL_USER": "aqt_user",
    #         "MYSQL_PASSWORD": "CoalescenceQWER!#$%0.",
    #         "MYSQL_CONNECTOR": "psycopg2"
    #     }
    # )
    dbDict = {
        "MYSQL_DB": "aqt_info_db",
        "MYSQL_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
        "MYSQL_USER": "aqt_user",
        "MYSQL_PASSWORD": "CoalescenceQWER!#$%0.",
        "MYSQL_CONNECTOR": "psycopg2"
    }
    ExampleModelHandler = create_data_table_class("ExampleModel",ExampleModel)
    # Step 3: Instantiate it — log should appear now
    handler_instance = ExampleModelHandler(dbDict)
    # handler_instance.data_update({"id": 2, "name": "Jason1", "created_at": datetime.datetime.utcnow()})
    user = handler_instance.data_get({"name": "Jason1"})
    logger.info(user)

def create_test_table_model():
    # Example usage of create_model_class
    tablename = "example1_table"
    schema = [
        {"name": "id", "type": "Integer", "primary_key": True, "autoincrement": True},
        {"name": "name", "type": "String", "length": 50, "nullable": True, "unique": True},
        {"name": "created_at", "type": "DateTime", "default": None}
    ]

    ExampleModel = create_model_class(tablename, schema)
    return ExampleModel

def create_test_table():
    ExampleModel = create_test_table_model()
    # Example usage of createTables
    dbDict = {
        "DB_DB": "aqt_info_db",
        "DB_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
        "DB_USER": "aqt_user",
        "DB_PASSWORD": "CoalescenceQWER!#$%0.",
        "DB_CONNECTOR": "psycopg2",
        "DB_SSL_MODE": "disable"
    }
    createTables(dbDict)
    logger.info(ExampleModel.__table__)
    logger.info(ExampleModel.__dict__)
    logger.info("Test table created successfully.")


def ingest_bulk_data():
    # Example usage of create_model_class
    ExampleModel = create_test_table_model()

    dbDict = {
        "DB_DB": "aqt_info_db",
        "DB_HOST": "aqt-info-db.c2l1ffm73dnf.us-east-1.rds.amazonaws.com",
        "DB_USER": "aqt_user",
        "DB_PASSWORD": "CoalescenceQWER!#$%0.",
        "DB_CONNECTOR": "psycopg2",
        "DB_SSL_MODE": "disable"
    }

    ExampleModelHandler = create_data_table_class("ExampleModel", ExampleModel)
    handler_instance = ExampleModelHandler(dbDict)
    ExampleModel.get_insert_table_info = classmethod(custom_get_insert_table_info)
    # Ingesting bulk data
    bulk_data = [
        {"name": "Alice", "created_at": datetime.datetime.now()},
        {"name": "b", "created_at": datetime.datetime.now()},
        {"name": "c", "created_at": datetime.datetime.now()}
    ]

    handler_instance.data_bulk_insert(bulk_data)
    logger.info("Bulk data ingested successfully.")

if __name__ == "__main__":
    ingest_bulk_data()