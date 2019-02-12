# -*- coding:utf-8 -*-
import traceback
import os
import tempfile
import simplejson
import subprocess, socket
import threading
import redis
import uuid
from functools import wraps
 
class TaskExecutor(object):
    def __init__(self, task_name ,  *args, **kwargs):
        self.queue =  redis.StrictRedis()#host='localhost', port=6378, db=0, password='xxx_tasks')
        self.task_name = task_name
        self.stdout_file = tempfile.mktemp() 
        self.stderr_file = tempfile.mktemp() 
 
    def _publish_task(self, task_id , func, *args, **kwargs):
        self.queue.lpush(self.task_name,
            # simplejson.dumps({'id':task_id, 'func':func, 'args':args, 'kwargs':kwargs})
            simplejson.dumps({'instance_id':uuid.uuid4().hex, 'task_id':task_id, 'execute_date':'201810', 'kwargs':kwargs})
        )

    def _publish_cmdtask(self, task_id , *args, **kwargs):
        print(args[0])
        self.queue.lpush(self.task_name,
            simplejson.dumps({'instance_id':6, 'task_id':'task_demo', 'execute_date':'201810', 'kwargs':kwargs})
        )
    def task(self, func):#decorator
        setattr(func,'delay',lambda *args, **kwargs:self._publish_task(uuid.uuid4().hex, func.__name__, *args, **kwargs))
        @wraps(func)
        def _w(*args, **kwargs):
            return func(*args, **kwargs)
        return _w

    def cmd(self,func):        
        setattr(func,'delay',lambda *args, **kwargs:self._publish_cmdtask(uuid.uuid4().hex, *args, **kwargs))
        @wraps(func)
        def _w(*args, **kwargs):
            if len(args):
                cmd=args[0]
                print(cmd)
            return func(*args, **kwargs)
        return _w

    def _read_task_log(self, stream):
        while True:
            line = stream.readline()
            if len(line) == 0:
                break
            print(str(line))

    def run_command(self):
        full_cmd = self.command
        print('Running Command: [{}]'.format(full_cmd))
        proc = subprocess.Popen(
            full_cmd,
            stdout=self.stdout_file,
            stderr=self.stderr_file,
            shell=True
        )
        # Start daemon thread to read subprocess log.loggerging output

        # logger_reader = threading.Thread(
        #     target=self._read_task_log,
        #     args=(proc.stdout,),
        # )
        # logger_reader.daemon = True
        # logger_reader.start()

        return proc
    def run(self):
        print ('waiting for tasks...')
        while True:
            if self.queue.llen(self.task_name):
                msg_data = simplejson.loads( self.queue.rpop(self.task_name))#这里可以用StrictRedis实例的brpop改善，去掉llen轮询。
                print('msg_data:{0}'.format(msg_data)) 
                print ('handling task(id:{0})...'.format(msg_data['id']))
                try:
                    if msg_data.get('func',None):
                        func = eval(msg_data.get('func'))
                        if callable(func):
                            #print msg_data['args'], msg_data['kwargs']
                            ret = func(*msg_data['args'], **msg_data['kwargs'])
                            msg_data.update({'result':ret})
                            self.queue.lpush(self.task_name+'.response.success', simplejson.dumps(msg_data) )
                    elif msg_data.get('cmd',None):
                        self.command=msg_data.get('cmd',None)
                        print('run cmd:{0}'.format(self.command))
                        self.run_command()
                        msg_data.update({'result':"success"})
                        self.queue.lpush(self.task_name+'.response.success', simplejson.dumps(msg_data) )        
                except:
                    msg_data.update({'failed_times':msg_data.get('failed_times',0)+1, 'failed_reason':traceback.format_exc()})
                    if msg_data.get('failed_times',0)<10:#最多失败10次，避免死循环
                        self.queue.rpush(self.task_name,simplejson.dumps(msg_data))
                    else:
                        self.queue.lpush(self.task_name+'.response.failure', simplejson.dumps(msg_data) )
                    print (traceback.format_exc())
 
 
PingTask = TaskExecutor('bi_scheduler_dispatch')
 
@PingTask.task
def ping_url(url):
    import os
    os.system('ping -c 2 '+url)

@PingTask.cmd
def cmd_demo(cmd):
    print('提交:{0}'.format(cmd))
if __name__=='__main__':
    # PingTask.run()
    cmd_demo.delay('ping')
