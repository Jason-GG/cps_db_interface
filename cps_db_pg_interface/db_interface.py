from abc import ABCMeta, abstractmethod
from sqlalchemy import exists, create_engine, and_, or_
from sqlalchemy.orm import relationship, sessionmaker  # 创建关系
from sqlalchemy.dialects.postgresql import insert as pg_insert
from typing import List, Dict
from .tool import logger
from . import tool
import threading
import copy

# Set up global locks and base class for SQLAlchemy models
global_lock = threading.Lock()
session_lock = threading.Lock()




class DataSqlTemplate(metaclass=ABCMeta):
    config = "default"

    def __init__(self, name, config) -> None:
        self.id = name
        self.config = config


    def create_gloabl_session(self):
        #   ******************************
        #   connection pool explaining :
        #   1, max connection number
        #   2, connection pool size
        #   3, connection waiting time
        #   4, reconnection time
        #   ******************************
        mysqlDic = self.config
        connectString = 'postgresql+' + mysqlDic['DB_CONNECTOR'] + '://' + mysqlDic['DB_USER'] + ':' + mysqlDic[
            'DB_PASSWORD'] + '@' + mysqlDic['DB_HOST'] + ':5432/' + mysqlDic['DB_DB'] + "?gssencmode="+mysqlDic['DB_SSL_MODE']
        logger.error(connectString)
        engine = create_engine(connectString, pool_size=20, max_overflow=30, pool_recycle=3600, pool_timeout=30,
                               pool_pre_ping=True)
        Session = sessionmaker(bind=engine)
        return Session


    def get_attr(self):
        return self.id

    @abstractmethod
    def data_get(self):  # DimensionsValue):
        pass

    @abstractmethod
    def data_update(self):
        pass

    @abstractmethod
    def data_delete(self):
        pass

    def clone(self):
        return copy.copy(self)

    @abstractmethod
    def data_get_info_from_client(self):
        pass


class DataTableBase(DataSqlTemplate):
    def __init__(self, name, config, table):
        super().__init__(name, config)
        self.table = table
        logger.info(f'table=====>>>>{config}')
        Session = self.create_gloabl_session()
        self.session = Session()

    def data_get(self, i=None):  # DimensionsValue):
        if self.table.get_attr() is None:
            return None

        logger.info(self.table.get_attr())
        logger.info(self.table.get_attr().__dict__["key"])
        res = self.session.query(self.table).filter(self.table.get_attr() == i[self.table.get_attr().__dict__["key"]]).first()
        r = res.__dict__
        del r["_sa_instance_state"]
        return r

    def data_update(self, args=None):
        if self.table.get_update_judge is None:
            return None
        logger.info(args)
        with session_lock:
            new_session = self.create_gloabl_session()
            self.session = new_session()
            if self.session.query(exists().where(self.table.get_update_judge(args=args))).scalar():
                tool.dic_delete_none_key(args)
                judgeArgs = dict(args)
                args.pop("id", None)
                logger.info(self.id)
                logger.error(judgeArgs)
                res = self.session.query(self.table).filter(self.table.get_update_judge(args=judgeArgs)).update(
                    judgeArgs)
                self.session.commit()
                logger.info("============>update")
                return res
            else:
                InfoTableItem = self.table.get_insert_table_info(args=args)
                logger.info(InfoTableItem)
                self.session.add(InfoTableItem)
                self.session.commit()
                logger.info("============>Insert")
                return InfoTableItem

    def data_bulk_insert(self, rows: List[Dict]):
        if not rows:
            logger.warning("No data provided for bulk insert.")
            return None
        try:
            with global_lock:
                logger.info("===========>>>>>> start bulk insert")
                # session = self.session
                # objects = [self.table.get_insert_table_info(args=row) for row in rows]
                # session.bulk_save_objects(objects)
                # session.commit()
                session = self.session
                # Directly insert raw row dictionaries (not ORM objects)
                insert_stmt = pg_insert(self.table.__table__).values(rows)
                # ON CONFLICT DO NOTHING — skip duplicates based on primary/unique keys
                do_nothing_stmt = insert_stmt.on_conflict_do_nothing()
                result = session.execute(do_nothing_stmt)
                session.commit()
                logger.info(f"insert {self.table.__tablename__} result: {result}")
                logger.info(f"Bulk inserted {len(rows)} rows into {self.table.__tablename__}")
                return rows
        except Exception as e:
            logger.error(f"Error during bulk insert: {e}")
            session.rollback()
        finally:
            session.close()
            logger.info("Session closed after bulk insert.")


    def data_update_from_id(self, args=None):
        if self.table.get_update_judge is None:
            return None
        logger.info(args)
        with session_lock:
            new_session = self.create_gloabl_session()
            self.session = new_session()
            # self.session.begin()
            if self.session.query(exists().where(self.table.get_update_judge_from_id(args=args))).scalar():
                tool.dic_delete_none_key(args)
                judgeArgs = dict(args)
                # args.pop("id", None)
                logger.info(self.id)
                logger.info(judgeArgs)
                res = self.session.query(self.table).filter(self.table.get_update_judge_from_id(args=judgeArgs)).update(
                    judgeArgs)
                self.session.commit()
                # self.safe_commit()
                logger.info("============>update")
                return res
            else:
                InfoTableItem = self.table.get_insert_table_info(args=args)
                logger.info(InfoTableItem)
                self.session.add(InfoTableItem)
                self.session.commit()
                # self.safe_commit()
                logger.info("============>Insert")
                return InfoTableItem

    def data_delete(self, i=None):
        if self.table.get_id() is None:
            return None
        res = self.session.query(self.table).filter(and_(self.table.get_id() == i)).update({'is_deleted': 1})
        return res

    def data_get_info_from_client(self):
        pass

    def safe_commit(self):
        if self.session:
            try:
                self.session.commit()
            except:
                self.session.rollback()
                raise

    def remove_session(self):
        if self.session:
            try:
                self.safe_commit()
            finally:
                # Session.remove()
                del self.session
                self.session = None

    def __del__(self):
        if self.session:
            self.session.commit()
            self.session.close()
            logger.info("Session closed in DataTableBase destructor.")


def create_data_table_class(name: str, table_class) -> type:
    """
    Dynamically create a DataTable class bound to the given SQLAlchemy table.
    :param name: Name of the resulting class
    :param table_class: A SQLAlchemy model (e.g., from create_model_class)
    :return: A new class inheriting from DataTableBase with `table` bound
    """
    logger.info(f'create_data_table_class → table_class: {table_class}')

    class BoundDataTable(DataTableBase):
        def __init__(self, config):
            logger.info(f'BoundDataTable.__init__ → table_class: {table_class}')
            logger.info(f'BoundDataTable.__init__ → table_class: {config}')
            super().__init__( name, config, table_class)

    BoundDataTable.__name__ = name
    return BoundDataTable

if __name__ == '__main__':
    pass
