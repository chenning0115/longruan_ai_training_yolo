import os, sys, subprocess
from contrib import utils as cutils
from contrib.taskdb import task_db_handler
from contrib.manage_stategy import check_running_status, check_if_should_run_next, dump_data_yaml, gen_params_str, get_weight_str
from contrib.manage_stategy import modify_task_status_to_running
from utils.general import LOGGER
import time

# base_prefix = os.path.dirname(os.path.abspath(__file__)).replace('\\', '/')
base_prefix = 'D://lr_videoai/data'
path_prefix_data_yaml = '%s/data_yaml' % base_prefix
path_prefix_log_path = "%s/log" % base_prefix
path_prefix_res_path = "%s/task/" % base_prefix



def real_run():
    loop_sign = cutils.time_now()
    LOGGER.info("--------------------%s LOOP CHECK.--------------------" % loop_sign)
    # 1. 检查running的任务状态并进行修改
    LOGGER.info('start to check running status...')
    check_running_status()

    # 2. 检查是否需要进行下一个新任务
    LOGGER.info('start to check if should run next...')
    check, new_task_id = check_if_should_run_next()
    if not check:
        LOGGER.info('[real_run]: no need to run next...')
        return
    
    LOGGER.info('need to run next task. will run...')

    # 3. run one task
    # 这里在启动任务前先设定为running状态 后续如果启动失败 则会被判定为failed
    modify_task_status_to_running(new_task_id)

    path_data = '%s/%s.yaml' % (path_prefix_data_yaml, new_task_id) 
    if cutils.check_file_exists(path_data) :
        LOGGER.info("%s, %s file exists, please check if task_id is conflict." % (new_task_id, path_data))
        return
    data_str = dump_data_yaml(path_data, new_task_id)
    params_str = gen_params_str(new_task_id)
    weight_str = get_weight_str(new_task_id)

    path_log = '%s/%s.txt' % (path_prefix_log_path, new_task_id)
    path_res = '%s/%s' % (path_prefix_res_path, new_task_id)
    if cutils.check_file_exists(path_log) or cutils.check_file_exists(path_res):
        LOGGER.info("%s, %s file exists, please check if task_id is conflict." % (path_log, path_res))
        return
    task_db_handler.set_log_path(new_task_id, path_log)
    task_db_handler.set_res_path(new_task_id, path_res)
    log_file = open(path_log, 'w')

    command_str = 'python train.py --taskid %s %s %s %s' % (new_task_id, params_str, data_str, weight_str)
    LOGGER.info('prepare to run task %s, run command is %s' % (new_task_id, command_str))
    process = subprocess.Popen(command_str, stdout=log_file, stderr=log_file)
    pid = process.pid
    task_db_handler.set_PID(new_task_id, pid)
    
    LOGGER.info('running task : task_id=%s, pid=%s, log_path=%s, res_path=%s' % (new_task_id, pid, path_log, path_res))

    LOGGER.info("-------------------------%s LOOP CHECK END.--------------------" % loop_sign)
    

if __name__ == "__main__":
    while True:
        real_run()
        time.sleep(5)