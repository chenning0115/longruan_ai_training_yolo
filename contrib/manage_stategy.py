import os, sys
sys.path.append('../')
from contrib import taskdb
import psutil
from utils.general import LOGGER

CODE_CHECK_SUCCESS = "ASKDJHDASLKGHLASDKHGFASGDLKASHLKFGDASLSADFGLKADSHGKLASDHGLKDSAHJFKDSAHGASJKGBHDKJASGHFKJASDGBFJKDASHGFKJASDHFKJASHFLGKAS"


#--------------状态转移------------------

# running 转移  SUCCESS, FAILED
# 返回 True/False, STATUS  是否需要转移，转移到哪个
def task_status_running_transfer(task_id):
    # 1. 判断是否是running状态 如果不是直接返回
    ss = taskdb.task_db_handler.get_status(task_id)
    if ss.strip() != taskdb.TASK_STATUS.RUNNING:
        LOGGER.info("task %s is not running. status=%s" % (task_id, ss))
    
    
    log_path = taskdb.task_db_handler.get_log_path(task_id)
    pid = taskdb.task_db_handler.get_PID(task_id)

    # 2. running下先判断是否success
    if os.path.exists(log_path):
        with open(log_path, 'r') as fin:
            ss = fin.read()[-10000: ]
            if CODE_CHECK_SUCCESS in ss:
                LOGGER.info("task %s is success, should change status to %s" % (task_id, taskdb.TASK_STATUS.SUCCESS))
                return True, taskdb.TASK_STATUS.SUCCESS

    def __check_pid_valid_and_exists(pid):
        try:
            pid = int(pid)
            return psutil.pid_exists(pid)
        except Exception as e:
            LOGGER.error("task %s pid is not valid. pid = %s" % (task_id, pid))
            return False

    # 3. running下 没有成功 但是pid不存在了 认为已经失败  
    if not __check_pid_valid_and_exists(pid):
        LOGGER.info("task %s is failed, should change status from running to %s" % (task_id, taskdb.TASK_STATUS.FAILED))
        return True, taskdb.TASK_STATUS.FAILED

    return False, taskdb.TASK_STATUS.WAIT



def modify_task_status_to_running(task_id):
    taskdb.task_db_handler.modify_status(task_id, taskdb.TASK_STATUS.RUNNING)

# 检查runnning状态的任务状态
def check_running_status():
    info_list = taskdb.task_db_handler.get_running_pids()
    for info in info_list:
        # 1. 当前为running状态
        task_id, pid = info
        # 2. running状态下判断是否需要状态转移
        check, status = task_status_running_transfer(task_id)
        if check:
            taskdb.task_db_handler.modify_status(task_id, status)


# check if should run next
# 当前判断只要没有任务在跑 就认为可以启动一个新任务 以后策略可以修改 支持多任务同时跑
def check_if_should_run_next():
    running_list = taskdb.task_db_handler.get_running_pids()
    if len(running_list) == 0:
        # should run next
        next_info_list = taskdb.task_db_handler.get_wait_list()
        print(next_info_list)
        if len(next_info_list) == 0:
            LOGGER.info('no waiting task...')
            return False, None
        else:
            task_id, rank = next_info_list[0]
            LOGGER.info('should run task_id=%s, its rank is %s' % (task_id, rank))
            return True, task_id
    else:
        LOGGER.info('still have running task...')
        return False, None

def dump_data_yaml(path, task_id):
    js = taskdb.task_db_handler.get_data_info(task_id)
    print('dump datafile into yaml. %s -> %s' % (task_id, path))
    with open(path, 'w') as fout:
        fout.write("# Train/val/test sets as 1) dir: path/to/imgs, 2) file: path/to/imgs.txt, or 3) list: [path/to/imgs1, path/to/imgs2, ..] \n")
        fout.write("path: %s\n" % js['path'])
        fout.write("train: %s\n" % js['train'])
        fout.write("val: %s\n" % js['val'])
        fout.write("test: %s\n" % js['test'])

        fout.write("names:\n")
        for k, v in js['names'].items():
            fout.write("    %s: %s\n" % (k, v))
        fout.flush()
    return "--data %s" % path


def gen_params_str(task_id):
    js = taskdb.task_db_handler.get_param_info(task_id)
    ss = ""
    for k, v in js.items():
        ss = ss + " --%s %s" % (k, v)
    return ss


def get_weight_str(task_id):
    js = taskdb.task_db_handler.get_weight_info(task_id)
    ss = "--weights %s" % js['path']
    return ss

if __name__ == "__main__":
    task_id = "6c377ad5e6f4406ebebeaa6c67fcfa91"
    pid = 8112
    