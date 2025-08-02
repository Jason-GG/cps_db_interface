from abc import ABCMeta, abstractmethod
from sqlalchemy import distinct
import copy
from sqlalchemy import exists, create_engine, and_, or_
from sqlalchemy.orm import relationship, sessionmaker  # 创建关系
from typing import List, Dict
import logging
import datetime
import gaget
import threading
import time

global_lock = threading.Lock()
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s  - %(message)s')
logger = logging.getLogger(__name__)


def create_gloabl_session(configPath):
    #   ******************************
    #   connection pool explaining :
    #   1, max connection number
    #   2, connection pool size
    #   3, connection waiting time
    #   4, reconnection time
    #   ******************************
    # print("==============================>>>>>>self.configPath", self.configPath)
    mysqlDic = configPath == "default" and gaget.deal_config() or gaget.deal_auth_config()
    # logging.debug(mysqlDic)
    connectString = 'mysql+' + mysqlDic['MYSQL_CONNECTOR'] + '://' + mysqlDic['MYSQL_USER'] + ':' + mysqlDic[
        'MYSQL_PASSWORD'] + '@' + mysqlDic['MYSQL_HOST'] + ':3306/' + mysqlDic['MYSQL_DB'] + '?charset=utf8mb4'
    logger.info(connectString)
    # engine = create_engine(connectString, max_overflow=100, pool_size=100, pool_timeout=10, pool_recycle=-1)
    engine = create_engine(connectString, pool_size=10,
                           max_overflow=5,
                           pool_recycle=3600,
                           pool_timeout=30,
                           echo=True,
                           echo_pool=True, )
    Session = sessionmaker(bind=engine)
    return Session


class DataMysqlTemplate(metaclass=ABCMeta):
    configPath = "default"

    def create_gloabl_session(self):
        #   ******************************
        #   connection pool explaining :
        #   1, max connection number
        #   2, connection pool size
        #   3, connection waiting time
        #   4, reconnection time
        #   ******************************
        # print("==============================>>>>>>self.configPath", self.configPath)
        mysqlDic = self.configPath == "default" and gaget.deal_config() or gaget.deal_auth_config()
        # logging.debug(mysqlDic)
        connectString = 'mysql+' + mysqlDic['MYSQL_CONNECTOR'] + '://' + mysqlDic['MYSQL_USER'] + ':' + mysqlDic[
            'MYSQL_PASSWORD'] + '@' + mysqlDic['MYSQL_HOST'] + ':3306/' + mysqlDic['MYSQL_DB'] + '?charset=utf8mb4'
        logger.info(connectString)
        # engine = create_engine(connectString, max_overflow=100, pool_size=100, pool_timeout=10, pool_recycle=-1)
        engine = create_engine(connectString, pool_size=20, max_overflow=30, pool_recycle=3600, pool_timeout=30,
                               pool_pre_ping=True)
        # echo = True, echo_pool = True,)
        # Session = sessionmaker(bind=engine, autocommit=True)
        Session = sessionmaker(bind=engine)
        return Session

    def __init__(self, name) -> None:
        # Session = self.create_gloabl_session()
        # self.session = Session()
        self.id = name
        # self.configPath = "default"

    # def creat_session(self):
    #     return self.create_gloabl_session

    def get_id(self):
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


session_lock = threading.Lock()


class DataTableBase(DataMysqlTemplate):
    def __init__(self, name, table):
        super().__init__(name)
        self.table = table
        Session = self.create_gloabl_session()
        self.session = Session()

    def data_get(self, i=None):  # DimensionsValue):
        # class 'sqlalchemy.orm.attributes.InstrumentedAttribute'
        if self.table.get_id() is None:
            return None

        logger.info(self.table.get_id())
        logger.info(self.table.get_id().__dict__["key"])
        res = self.session.query(self.table).filter(
            self.table.get_id() == i[self.table.get_id().__dict__["key"]]).first()
        r = res.__dict__
        del r["_sa_instance_state"]
        return r

    def data_update(self, args=None):
        # self.safe_commit()
        # class 'sqlalchemy.orm.attributes.InstrumentedAttribute'
        # print(self.table.get_update_info()[0], type(self.table.get_update_info()[0]),
        #       self.table.get_update_info()[0].__dict__["key"])
        # time.sleep(1)
        if self.table.get_update_judge is None:
            return None
        # logger.info("===========================>>>>  data_update:" + self.configPath)
        logger.info(args)
        with session_lock:
            # new_session = create_gloabl_session(self.configPath)
            new_session = self.create_gloabl_session()
            self.session = new_session()
            # self.session.begin()
            if self.session.query(exists().where(self.table.get_update_judge(args=args))).scalar():
                gaget.dic_delete_none_key(args)
                judgeArgs = dict(args)
                args.pop("id", None)
                logger.info(self.id)
                logger.info(judgeArgs)
                res = self.session.query(self.table).filter(self.table.get_update_judge(args=judgeArgs)).update(
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

    def data_bulk_insert(self, rows: List[Dict]):
        if not rows:
            logger.warning("No data provided for bulk insert.")
            return None
        try:
            with global_lock:
                logger.info("===========>>>>>> start bulk insert")
                session = self.session
                objects = [self.table.get_insert_table_info(args=row) for row in rows]
                session.bulk_save_objects(objects)
                session.commit()
                logger.info(f"Bulk inserted {len(objects)} rows into {self.table.__tablename__}")
                return objects
        except Exception as e:
            logger.error(f"Error during bulk insert: {e}")
            session.rollback()
        finally:
            session.close()
            logger.info("Session closed after bulk insert.")


    def data_update_from_id(self, args=None):
        # self.safe_commit()
        # class 'sqlalchemy.orm.attributes.InstrumentedAttribute'
        # print(self.table.get_update_info()[0], type(self.table.get_update_info()[0]),
        #       self.table.get_update_info()[0].__dict__["key"])
        # time.sleep(1)
        if self.table.get_update_judge is None:
            return None
        # logger.info("===========================>>>>  data_update:" + self.configPath)
        logger.info(args)
        with session_lock:
            # new_session = create_gloabl_session(self.configPath)
            new_session = self.create_gloabl_session()
            self.session = new_session()
            # self.session.begin()
            if self.session.query(exists().where(self.table.get_update_judge_from_id(args=args))).scalar():
                gaget.dic_delete_none_key(args)
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
    # def __del__(self):
    #     if self.session:
    #         self.remove_session()


if __name__ == '__main__':
    from cloud_watch_efs_metrics_sql import EfsStorageUtilization

    p = DataTableBase("EfsStorageUtilization", EfsStorageUtilization)
    p.data_update(args={"Timestamp": "2022-12-01", "Data": 14, "Unit": "bytes", "efs_juction_id": 1})
