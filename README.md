# 慢日志实时收集
说明：本模块为慢日志收集平台的基础组件-实时收集

## 如何部署
1.环境要求
- python3.7
- mysql数据库（存慢日志）
- FastAPI

2.如何部署

**2.1 服务端**

代码部署

```
git@github.com:xuclachina/slow_log_platform.git
```

安装依赖

```
cd slow_log_platform/server

pip install -r requirements.txt
```

启动服务

```
uvicorn main:app --port=8000 --host=0.0.0.0 &
```

查看端口

```
[root@i-qjxa18sy server]# netstat -lntp|grep 8000
tcp        0      0 0.0.0.0:8000            0.0.0.0:*               LISTEN      15333/python3.8
```

**2.2 agent**

代码部署

```
git@github.com:xuclachina/slow_log_platform.git
```

安装依赖

```
cd slow_log_platform/agent

pip install -r requirements.txt
```

修改配置文件

```
#mysql部分，暂时用不到
[mysql]
host = 127.0.0.1
user = root
password = 123456

#server地址：端口
[server]
url = http://10.105.6.13:8000/v1/slowlog/

#慢日志目录，max_size暂时也用不到
[slowlog]
filename = /storage/mysql/mysql3306/data/slow.log 
max_size = 1073741824

#对应cmdb中的实例id
[instance]
dbid = 1

#元数据保存目录
[meta]
dir = .
```



supervisor配置

请参考：https://www.jianshu.com/p/2f032b52c84a

配置文件

```
[root@localhost conf]# cat /etc/supervisor.d/slowlog.ini
[program:slowlog]
directory = /storage/slowlog_platform/agent ; 
command = python slow_log_parser.py ;
autostart = true     ;
startsecs = 5        ;
autorestart = true   ;
startretries = 3     ;
redirect_stderr = true  ;
stdout_logfile_maxbytes = 20MB  ;
stdout_logfile_backups = 20     ;
```

启动supervisor





