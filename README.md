该项目基于[cflow](https://github.com/lanfang/cflow)主体调度系统，做适当修改：
- kafka->Redis
- etl_day时间格式yyyy-mm-dd->yyyymmdd
- 加入web管理调度任务模块，降低使用难度

# goflow
 用python实现的高可用可横向扩展的分布式调度系统,并具有可视化操作的功能

# 包含功能
- 定时调度(类似linux crontab)
- 依赖调度(满足依赖关系后才会启动任务)
- 任务格式: 任意命令行可执行程序
- 高可用，可横向扩展(调度器和执行器均可同时运行多个，完善的重试机制)
- 可视化操作（基于flask）

# 系统完整流程
> 调度器获取任务，通过Redis进行任务分发，执行器消费Redis的任务并执行

![scheduler.png](https://github.com/importer/goflow/raw/master/docs/scheduler.png)

# web页面处理调度任务
## 主界面
![web_scheduler.PNG](https://github.com/importer/goflow/raw/master/docs/web_scheduler.PNG)
## 任务执行情况及任务操作
![TaskRunEdit.PNG](https://github.com/importer/goflow/raw/master/docs/TaskRunEdit.PNG)
任务操作：
- 删除
- 强制执行
- 重做当前
- 重做当前及后续
- 强制通过

## 任务执行日志输出
![TaskRunLog.PNG](https://github.com/importer/goflow/raw/master/docs/TaskRunLog.PNG)


### TODO
- 查看单个任务依赖关系
- 优化任务触发方式
- 修复未知bug
- 用户权限
- 任务路由
- 任务优先级
- work发现
# 使用方法:
> goflow命令位于工程目录下


- 安装mysql和etcd，执行 goflow init_db -c goflow_conf.json
- 运行调度器: goflow scheduler -c goflow_conf.json --ha
- 运行执行器(可启多个): goflow executer -c goflow_conf.json


# 配置文件介绍
> 配置文件为json格式
```
{
  "Common":{
    "MysqlConn":"mysql://test:test@localhost:3306/goflow", # 数据库地址
    "MysqlConn":"mysql://test:test@localhost:3306/cflow", # 数据库地址
    "Broker":"localhost:9092"   # kafka地址 也可基于redis
  },
  "Scheduler":{
    "LogDir":"/var/log/go_log", # 日志路径
    "LogFile":"scheduler.log", # 日志文件
    "FetchInterval":10, # 扫描cron_conf间隔(秒)
    "WorkerTimeOut":20, # woker 心跳超时时间
    "RetryQueueimes":4  # 超时任务，重新发起调度的次数
  },
  "Worker": {
    "LogDir":"/var/log/go_log",
    "LogFile":"executer.log",
    "Parallelism": 32, # 每个woker，并行执行任务的个数
    "HearteatInterval": 10, # worker 心跳间隔(秒)
    "Retry": 3, # 单个任务重试次数
    "TaskTimeOut": 60 # 执行单个任务时的超时时间(秒), -1 不设置超时
  }
}
```


# 库表介绍(database: goflow)
- cron_conf crontab 配置表，存储定时任务配置
- loader_result 抽取结果表，存抽取结果，以及worker直接文件同步
- stat_result 每天的执行结果
- task_define 普通任务定义
- task_dependency 任务依赖关系
- task_instance 所有的任务实例, 记录任务的运行状态信息等


# 常用命令介绍(goflow)
## 子命令列表 goflow -h:
```
{run,dep,do_all_job,migrate,version,kill,executer,scheduler,init_db} ...
```
## 重跑命令(goflow run)
### goflow run -h
```
查看参数列表和使用方法
```

## 数据初始化(init_db -c )
### goflow init_db -c goflow_conf.json
```
查看参数列表和使用方法
```

### 重跑单个任务(需满足依赖关系)
```
goflow  run -j task_id -d YYYY-MM-DD  
```

### 重跑单个任务(无需满足依赖)
```
goflow  run -j task_id -d YYYY-MM-DD --force 
```

### 重跑任务以及其后置任务
```
goflow  run -j task_id -d YYYY-MM-DD -down
```

### 重跑任务以及其前置任务
```
goflow  run -j task_id -d YYYY-MM-DD -up
```
  

### www页面处理调度任务
任务操作：
开始
强制执行
重做当前
重做当前及后续
强制通过
### TODO
1、查看依赖
2、优化任务触发方式
3、修复未知bug
