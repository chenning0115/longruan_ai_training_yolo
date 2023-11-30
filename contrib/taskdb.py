import os, sys
import json


import psycopg2



class TASK_STATUS:
    SUCCESS = 'SUCCESS'
    FAILED = 'FAILED'
    WAIT = 'WAIT'
    RUNNING = 'RUNNING'
    READYFORRUN = 'READYFORRUN'


class TaskDBHandler(object):
    '''
    caution: 本类必须要是无状态的
    '''
    def __init__(self) -> None:
        self.db = 'videoai'
        self.table_name = 'lry_videoaitaskmanage'
        config={'host':'10.10.10.244', 'user':'local', 'password':'Longruan@123', 'database':self.db, 'autocommit':True}
        self.pool =  psycopg2.connect(database=self.db, user="local", password="Longruan@123", host="10.10.10.244", port="5432")

        self.temp_pid = None
        self.temp_log_path = None
        self.temp_res_path = None

    
    def get_conn(self):
        # conn = self.pool.get_connection()
        conn = psycopg2.connect(database=self.db, user="local", password="Longruan@123", host="10.10.10.244", port="5432")
        return conn

    def get_attr(self, task_id, attr):
        conn = self.get_conn()
        cursor = conn.cursor()  # 创建游标对象
        sql = "select %s from %s where id='%s'" % (str(attr), self.table_name, task_id)
        # print('[get info sql] %s ' % sql)
        cursor.execute(sql)  # 执行SQL查询语句
        result = cursor.fetchall()  # 获取查询结果
        cursor.close()  # 关闭游标对象
        conn.close()
        return result

    def try_load_json(self, raw):
        try:
            return json.loads(raw)
        except:
            return {}

    def get_status(self, task_id):
        result = self.get_attr(task_id, 'status')
        ss = ""
        if len(result) > 0:
            ss = result[0][0]
        # print(type(ss), ss)
        return ss

    def get_data_info(self, task_id):
        '''
        return: json
        '''
        # demo = {
        #     'path': '../dataset/coal_miner_data2023_yolo',   # dataset root dir
        #     'train': 'images/train',
        #     'val': 'images/train',
        #     'test': '',
        #     'names': {
        #         0: 'coalman'
        #     }
        # }        
        # return demo
        result = self.get_attr(task_id, 'data')
        ss = {}
        if len(result) > 0:
            ss = self.try_load_json(result[0][0])
        # print(type(ss), ss)
        return ss

    def get_param_info(self, task_id):
        result = self.get_attr(task_id, 'param')
        ss = {}
        if len(result) > 0:
            ss = self.try_load_json(result[0][0])
        # print(type(ss), ss)
        return ss

    def get_weight_info(self, task_id):
        result = self.get_attr(task_id, 'weight')
        ss = {}
        if len(result) > 0:
            ss = self.try_load_json(result[0][0])
        # print(type(ss), ss)
        return ss

    def set_attr(self, task_id, attr, val):
        conn = self.get_conn()
        cursor = conn.cursor()  # 创建游标对象
        sql = '''UPDATE  %s SET %s = '%s' WHERE id = '%s'; ''' % (
            self.table_name, attr, val, task_id) 
        print('[update sql] %s ' % sql)
        cursor.execute(sql)  # 执行SQL查询语句
        conn.commit() # 提交更改
        cursor.close()  # 关闭游标对象
        conn.close()

    def modify_status(self, task_id, status):
        self.set_attr(task_id, 'status', status)
        # print('[update status=%s of %s success.]' % (status, task_id))


    def set_PID(self, task_id, pid):
        self.set_attr(task_id, 'process_id', pid)
        # print('[update pid=%s of %s success.]' % (pid, task_id))

    def get_PID(self, task_id):
        result = self.get_attr(task_id, 'process_id')
        ss = -1
        if len(result) > 0:
            ss = result[0][0]
        # print(type(ss), ss)
        return ss

    def get_log_path(self, task_id):
        result = self.get_attr(task_id, 'log_path')
        ss = ''
        if len(result) > 0:
            ss = result[0][0]
        # print(type(ss), ss)
        return ss

    def get_res_path(self, task_id):
        result = self.get_attr(task_id, 'res_path')
        ss = ''
        if len(result) > 0:
            ss = result[0][0]
        # print(type(ss), ss)
        return ss

    def set_data(self, task_id, data_info):
        '''
        data_info: json
        '''
        ss = json.dumps(data_info)
        self.set_attr(task_id, 'data', ss)

    def set_param(self, task_id, params):
        '''
        params: json
        '''
        ss = json.dumps(params)
        self.set_attr(task_id, 'param', ss)

    def set_weight(self, task_id, weight):
        '''
        weight: json
        '''
        ss = json.dumps(weight)
        self.set_attr(task_id, 'weight', ss)



    def set_log_path(self, task_id, log_path):
        '''
        log_path: str
        '''
        self.set_attr(task_id, 'log_path', log_path)
        # print('[update log_path=%s of %s success.]' % (log_path, task_id))

    def set_res_path(self, task_id, res_path):
        '''
        res_path : str
        '''
        self.set_attr(task_id, 'res_path', res_path)
        # print('[update res_path=%s of %s success.]' % (res_path, task_id))

    def set_rank(self, task_id, rank):
        '''
        '''
        self.set_attr(task_id, 'rank', rank)


    # -------------------------- 
    # 针对全表扫描

    def get_running_pids(self):
        '''
        get pidlist of running tasks
        return : [[task_id, pid], [task_id, pid], ...]
        '''
        sql = "select id, process_id from %s where status='%s'" % (self.table_name, TASK_STATUS.RUNNING)
        conn = self.get_conn()
        cursor = conn.cursor()  # 创建游标对象
        cursor.execute(sql)  # 执行SQL查询语句
        result = cursor.fetchall()  # 获取查询结果
        res = []
        for row in result:
            res.append([row[0], row[1]])
        cursor.close()  # 关闭游标对象
        conn.close()
        return result

    def get_wait_list(self, order_by_rank=True):
        '''
        获取等待列表 并按照rank从小到大排序
        返回pids
        return : [[task_id, rank], [task_id, rank ], ...]
        '''
        sql = "select id, %s.rank from %s where status='%s' order by %s.rank DESC, create_date; " \
            % (self.table_name, self.table_name, TASK_STATUS.WAIT, self.table_name)
        conn = self.get_conn()
        cursor = conn.cursor()  # 创建游标对象
        cursor.execute(sql)  # 执行SQL查询语句
        result = cursor.fetchall()  # 获取查询结果
        res = []
        for row in result:
            res.append([row[0], int(row[1])])
        cursor.close()  # 关闭游标对象
        conn.close()
        return res

        # return [["6c377ad5e6f4406ebebeaa6c67fcfa91", 0]]


task_db_handler = TaskDBHandler()


def test_get():
    task_id = 'bf0b670fdc8349f2a85c05f38a6b2333'
    task_db_handler.get_data_info(task_id)
    task_db_handler.get_param_info(task_id)
    task_db_handler.get_weight_info(task_id)


def test_set():
    task_id = 'bf0b670fdc8349f2a85c05f38a6b2333'
    task_db_handler.set_PID(task_id, '001')

    data_info = {
        # "path": "../dataset/coal_miner_data2023_yolo",
        "path": "../dataset/coal_miner_test",
        "train": "images/train",
        "val": "images/train",
        "test": "", 
        "names": {
            0: "coalman"
        }
    }
    task_db_handler.set_data(task_id, data_info)

    weight = {
        "path": "yolov5s.pt"
    }
    task_db_handler.set_weight(task_id, weight)

    params = {
        "batch-size": 64,
        "epochs": 30 
    }
    task_db_handler.set_param(task_id, params)

    task_db_handler.set_rank(task_id, 9)
    task_db_handler.modify_status(task_id, TASK_STATUS.WAIT)


if __name__ == "__main__":
    test_set()
    test_get() 
    print(task_db_handler.get_running_pids())
    print(task_db_handler.get_wait_list())