from sqlalchemy import Column, Integer, String, DateTime, Boolean, Float, Text, JSON, DATE, BigInteger
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import exists, create_engine, and_, or_
from .tool import logger

Base = declarative_base()

# Mapping of type string to actual SQLAlchemy types
SQLALCHEMY_TYPE_MAP = {
    "Integer": Integer,
    "String": String,
    "Text": Text,
    "DateTime": DateTime,
    "DATE": DATE,
    "Boolean": Boolean,
    "Float": Float,
    "JSON": JSON,
    "BigInteger": BigInteger,
}

class DynamicTableMixin:
    @classmethod
    def get_id(cls):
        return getattr(cls, 'id', None)

    @classmethod
    def get_update_judge(cls, args=None):
        if "id" not in args:
            return None
        return and_(getattr(cls, "id") == args["id"])

    @classmethod
    def get_insert_table_info(cls, args=None):
        return cls(**args)

    def as_dict(self):
        return {column.name: getattr(self, column.name) for column in self.__table__.columns}

    def __repr__(self):
        values = ", ".join(f"{k}={v!r}" for k, v in self.__dict__.items() if not k.startswith('_'))
        return f"<{self.__class__.__name__}({values})>"


def create_model_class(tablename, schema):
    attrs = {"__tablename__": tablename}
    for field in schema:
        col_type = SQLALCHEMY_TYPE_MAP[field["type"]]
        if field["type"] == "String" and "length" in field:
            col_type = col_type(field["length"])

        attrs[field["name"]] = Column(
            col_type,
            primary_key=field.get("primary_key", False),
            autoincrement=field.get("autoincrement", False),
            nullable=field.get("nullable", True),
            unique=field.get("unique", False),
            default=field.get("default", None)
        )

    return type(tablename.capitalize(), (Base, DynamicTableMixin), attrs)


def createTables(mysqlDic):
    logger.error(mysqlDic)
    connectString = 'postgresql+' + mysqlDic['DB_CONNECTOR'] + '://' + mysqlDic['DB_USER'] + ':' + mysqlDic[
        'DB_PASSWORD'] + '@' + mysqlDic['DB_HOST'] + ':5432/' + mysqlDic['DB_DB'] + "?gssencmode=" + mysqlDic['DB_SSL_MODE']
    logger.error(connectString)
    engine = create_engine(connectString,
                           max_overflow=0,  # 超过连接池大小外最多创建的连接
                           pool_size=5,  # 连接池大小
                           pool_timeout=30,  # 池中没有线程最多等待的时间，否则报错
                           pool_recycle=-1  # 多久之后对线程池中的线程进行一次连接的回收(#重置)
                           )
    ## 新建所有的表
    Base.metadata.create_all(engine)