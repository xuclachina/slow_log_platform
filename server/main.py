#!/usr/bin/env python
# -*- encoding: utf-8 -*-
'''
@File    :   main.py
@Time    :   2020/03/04 09:20:30
@Author  :   xuchenliang
@Desc    :   None
'''

from fastapi import FastAPI, Body
from pydantic import BaseModel
import databases
import sqlalchemy

DATABASE_URL = "mysql://zdbslow:RvFn6wet9svfIVsH@127.0.0.1/slowlogs"
database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

slowlogs = sqlalchemy.Table(
    "slowlogs",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("dbid", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("db_user", sqlalchemy.String(30), nullable=False),
    sqlalchemy.Column("app_ip", sqlalchemy.String(30), nullable=False),
    sqlalchemy.Column("thread_id", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("exec_duration", sqlalchemy.String(20), nullable=False),
    sqlalchemy.Column("rows_sent", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("rows_examined", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("start_time", sqlalchemy.Integer, nullable=False),
    sqlalchemy.Column("sql_pattern", sqlalchemy.Text, nullable=False),
    sqlalchemy.Column("orig_sql", sqlalchemy.Text, nullable=False),
    sqlalchemy.Column("fingerprint", sqlalchemy.String(50), nullable=False),
)

engine = sqlalchemy.create_engine(
    DATABASE_URL
)
metadata.create_all(engine)


class SlowLog(BaseModel):
    dbid: int
    db_user: str
    app_ip: str
    thread_id: int
    exec_duration: float
    rows_sent: int
    rows_examined: int
    start_time: str
    sql_pattern: str
    orig_sql: str
    fingerprint: str


app = FastAPI()


@app.on_event("startup")
async def startup():
    await database.connect()


@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.post('/v1/slowlog')
async def post_slowlog(
        item: SlowLog = Body(
            ...,
            example={
                "dbid": "1",
                "db_user": " monitor",
                "app_ip": "localhost",
                "thread_id": 331578,
                "exec_duration": "0.000441",
                "rows_sent": 1,
                "rows_examined": 154,
                "start_time": 1583219343,
                "orig_sql": "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME =queries LIMIT 1;",
                "sql_pattern": "SELECT VARIABLE_VALUE FROM performance_schema.global_status WHERE VARIABLE_NAME =? LIMIT ?;",
                "fingerprint": "f97d833b6117df9487142b39b5454293"
            },
        )
):
    query = slowlogs.insert().values(
        dbid=item.dbid,
        db_user=item.db_user,
        app_ip=item.app_ip,
        thread_id=item.thread_id,
        exec_duration=item.exec_duration,
        rows_sent=item.rows_sent,
        rows_examined=item.rows_examined,
        start_time=item.start_time,
        orig_sql=item.orig_sql,
        sql_pattern=item.sql_pattern,
        fingerprint=item.fingerprint)
    last_record_id = await database.execute(query)
    return {**item.dict(), "id": last_record_id}
