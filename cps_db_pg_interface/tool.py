import json
import os
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(module)s - %(funcName)s  - %(message)s')
logger = logging.getLogger(__name__)


class _const:
    class ConstError(TypeError): pass

    def __setattr__(self, name, value):
        if name in self.__dict__:
            raise self.ConstError("Can't rebind const (%s)" % name)
        self.__dict__[name] = value


const = _const()
# linux环境和windows环境进行切换
const.SYSTEM = "Linux"

def path_mysql_config():
    # systemType = sys.platform
    if const.SYSTEM == "Windows":
        file = os.path.split(os.path.realpath(__file__))[0] + "\\mysql\\sql_info.json"  # 获取当前工作目录路径
    else:
        file = os.path.split(os.path.realpath(__file__))[0] + "/rds_resources/mysql/sql_info.json"  # 获取当前工作目录路径
    # print("------------------file", file)
    return file

def load(json_filename):
    with open(json_filename) as json_file:
        data = json.load(json_file)
        return data

def deal_config(file_path=None):
    file = file_path
    dic = load(file)
    print(os.environ.get('ENV_EXE'))
    if dic:
        return dic[os.environ.get('ENV_EXE')]
    return None

def dic_delete_none_key(dic):
    keys_to_delete = [k for k, v in dic.items() if v is None]
    for k in keys_to_delete:
        del dic[k]