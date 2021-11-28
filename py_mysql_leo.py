# -*- coding:utf-8 -*-
# @author LeoWang
# @date 2021/11/26
# @file py_mysql_leo.py
import pymysql
from pprint import pprint


# conn = pymysql.connect()
def createDatabase(create_name, show=0, **kwargs):
    """
    创建数据库

    :param create_name: 创建数据库的名称
    :param show: 是否打印所有数据库
    :param kwargs: 关键字参数, 接受需要更新或添加的参数
    :return: 成功:1 失败:0
    """
    # 默认参数
    _config = {
        'host'   : "localhost",
        'port'   : 3306,
        'user'   : "root",
        'passwd' : "031214",
        'charset': 'utf8'
    }
    # # 添加字典
    # for key in kwargs:
    #     if key not in config:
    #         config[key] = kwargs[key]

    # # 更新字典
    for key in kwargs:
        _config[key] = kwargs[key]

    try:
        # 连接数据库
        connect = pymysql.connect(**_config)
        # 获取游标对象
        cursor = connect.cursor()
        # 执行SQL命令
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {create_name} DEFAULT CHARSET {_config['charset']}")
        # cursor.execute(f"CREATE DATABASE {create_name}")
        # 提交事务
        connect.commit()

        if show:
            showDatabase()
            # print('现在系统中数据库为:')
            # cursor.execute("SHOW DATABASES}")
            # all = cursor.fetchall()
            # for i in all:
            #     print(i[0])

        # 关闭游标
        cursor.close()
        # 关闭连接
        connect.close()
        print('创建成功')
        return 1
    except:
        print('创建失败!')
        return 0

    # config = {
    #     'host'   : 'localhost',
    #     'port'   : 3306,
    #     'user'   : 'root',
    #     'passwd' : '031214',
    #     'db'     : 'test_jeff',
    #     'charset': 'utf8'
    # }
    #
    # try:
    #     # 打开数据库连接
    #     conn = pymysql.connect(**config)
    #
    #     # 使用 cursor() 方法创建一个游标对象 cursor
    #     cursor = conn.cursor()
    #
    #     # 使用 execute() 方法执行 SQL，如果表存在则删除
    #     cursor.execute("CREATE DATABASE IF NOT EXISTS pythonDB DEFAULT CHARSET utf8")
    #     conn.commit()
    #     # # 使用预处理语句创建表
    #     # sql = """CREATE TABLE EMPLOYEE (
    #     #          FIRST_NAME  CHAR(20) NOT NULL,
    #     #          LAST_NAME  CHAR(20),
    #     #          AGE INT,
    #     #          SEX CHAR(1),
    #     #          INCOME FLOAT,
    #     #          PRIMARY KEY (FIRST_NAME)
    #     #          )"""
    #     # cursor.execute(sql)
    #     # # 关闭游标
    #     cursor.close()
    #     conn.close()
    #     print("创建成功")
    # except Exception:
    #     print("创建失败")


def showDatabases(mode='0', wrap=1, **kwargs):
    """
    打印所有数据库

    :param mode:
        (默认)-1: 以列表形式打印
        0: 以换行带缩进打印
        else: 以参数为分割符打印
    :param wrap: 打印结束后是否打印换行
    :param kwargs: 关键字参数, 用来接收补充参数
    :return: 成功返回数据库列表, 失败返回 0
    """

    # 默认参数
    _config = {
        'host'   : "localhost",
        'port'   : 3306,
        'user'   : "root",
        'passwd' : "031214",
        'charset': 'utf8'
    }
    # 更新字典
    for key in kwargs:
        _config[key] = kwargs[key]

    try:
        # 连接数据库
        connect = pymysql.connect(**_config)
        # 获取游标对象
        cursor = connect.cursor()
        # 执行SQL命令

        cursor.execute("SHOW DATABASES")
        all_databases = cursor.fetchall()
        all_databases = [i[0] for i in all_databases]
        # all_databases.sort()
        # print(all_databases)
        print('系统中数据库为:')
        if mode == '-1':
            print(all_databases)
        elif mode == '0':
            for i in all_databases:
                print(f'\t{i}')
        else:
            for i in all_databases:
                print(i, end=mode)
        if wrap:
            print()
        # 关闭游标
        cursor.close()
        # 关闭连接
        connect.close()
        return all_databases
    except:
        return 0


def databaseExist(database_name):
    """
    判断数据库是否存在

    :param database_name: 需判断的数据库名称
    :return: 是(1)否(0)存在
    """
    _config = {
        'host'    : "localhost",
        'port'    : 3306,
        'user'    : "root",
        'passwd'  : "031214",
        'charset' : 'utf8',
        'database': database_name
    }

    try:
        # 连接数据库
        connect = pymysql.connect(**_config)
        cursor = connect.cursor()
        cursor.close()
        connect.close()
        return 1
    except:
        return 0


# def createTable(database_name,table_name,primary_key,**kwargs):
#     pass
#     # 默认参数
#     _config = {
#         'host'   : "localhost",
#         'port'   : 3306,
#         'user'   : "root",
#         'passwd' : "031214",
#         'charset': 'utf8'
#     }
#     # 更新字典
#     table_config = ''
#     for key in kwargs:
#         field_type = kwargs[key]
#         table_config += f'{key} {field_type},'
#     table_config = table_config[:-1]
#     table_config = f'({table_config})'
#     print(table_config)
#     # try:
#     #     # 连接数据库
#     #     connect = pymysql.connect(**_config)
#     #     # 获取游标对象
#     #     cursor = connect.cursor()
#     #     # 执行SQL命令
#     #
#     #     cursor.execute("SHOW DATABASES")
#     #     all_databases = cursor.fetchall()
#     #     all_databases = [i[0] for i in all_databases]
#     #     # all_databases.sort()
#     #     # print(all_databases)
#     #     print('现在系统中数据库为:')
#     #     if mode == '-1':
#     #         print(all_databases)
#     #     elif mode == '0':
#     #         for i in all_databases:
#     #             print(f'\t{i}')
#     #     else:
#     #         for i in all_databases:
#     #             print(i, end=mode)
#     #     if wrap:
#     #         print()
#     #     # 关闭游标
#     #     cursor.close()
#     #     # 关闭连接
#     #     connect.close()
#     #     return 1
#     # except:
#     #     return 0


def selectTableValue(database_name, table_name, field_name='', value='', rank='*', show=0, **kwargs):
    """
    选择数据

    :param database_name: 数据库名称
    :param table_name: 表名
    :param field_name: 待查字段
    :param value: 待查值
    :param rank: 选择列, 默认为 *
    :param show: 选择后是否打印, 默认不打印
    :param kwargs: 更新参数
    :return: 成功返回选择结果, 失败返回 0
    """
    # 默认参数
    _config = {
        'host'    : "localhost",
        'port'    : 3306,
        'user'    : "root",
        'passwd'  : "031214",
        'charset' : 'utf8',
        'database': database_name
    }

    # 更新字典
    for key in kwargs:
        _config[key] = kwargs[key]

    try:
        # 连接数据库
        connect = pymysql.connect(**_config)
        # 获取游标对象
        cursor = connect.cursor()
        # 执行SQL命令
        if value == '':
            cursor.execute(f"SELECT {rank} FROM {table_name}")
        else:
            cursor.execute(f"SELECT {rank} FROM {table_name} WHERE {field_name} = '{value}' ")

        all_value = cursor.fetchall()

        # all_value = [i[0] for i in all_value]
        # # all_value.sort()
        # # print(all_value)
        # for i in all_value:
        #     print(f'\t{i}')
        if show == 1:
            pprint(all_value)
        # 关闭游标
        cursor.close()
        # 关闭连接
        connect.close()
        return all_value
    except:
        return 0

    pass


def deleteValue(database_name, table_name, field_name, del_value, **kwargs):
    """
    删除数据

    :param database_name: 数据库名称
    :param table_name: 表名
    :param field_name: 字段
    :param del_value: 待删值
    :param kwargs: 关键字参数, 默认参数
    :return: 删除成功: 被删数据, 删除失败: 0
    """
    # 默认参数
    _config = {
        'host'    : "localhost",
        'port'    : 3306,
        'user'    : "root",
        'passwd'  : "031214",
        'charset' : 'utf8',
        'database': database_name,
    }

    # 更新字典
    for key in kwargs:
        _config[key] = kwargs[key]

    try:
        # 连接数据库
        connect = pymysql.connect(**_config)
        # 获取游标对象
        cursor = connect.cursor()
        # 执行SQL命令
        # 获取值
        value = selectTableValue(database_name, table_name, field_name, del_value, show=1)
        # 删除值
        cursor.execute(f"DELETE FROM {table_name} WHERE {field_name} = '{del_value}' ")
        # 提交事务
        connect.commit()
        # 关闭游标
        cursor.close()
        # 关闭连接
        connect.close()
        return value
    except:
        return 0


__all__ = [
    "createDatabase",
    "showDatabases",
    "databaseExist"
]
if __name__ == '__main__':
    pass

    # createDatabase('test1', True)
    # showDatabases()
    # print(databaseExist('java_work'))
    selectTableValue('test', 'user', show=1)
    deleteValue('test', 'user', field_name='name', del_value='leo')
    selectTableValue('test', 'user', show=1)
    # createTable(1, 1, name='varchar(255)', age='int')
