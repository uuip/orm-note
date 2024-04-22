import datetime

import peewee as pw
from peewee import *

from pw.db import db


# 生成模型代码
# https://docs.peewee-orm.com/en/latest/peewee/playhouse.html#pwiz-a-model-generator


class OnChainLog(pw.Model):
    """记录已上链的操作日志"""

    logmodel_id = pw.BigIntegerField()
    request_id = pw.TextField()
    request_time = pw.DateTimeField(default=datetime.datetime.now)
    transaction = pw.TextField()

    class Meta:
        table_name = "logmodel_onchain"
        database = db


class Demo(pw.Model):
    class Meta:
        database = db
        table_name = "dungeon_ship"
        constraints = [SQL("UNIQUE(caddress,ship,round)")]

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.token_id}>"


db.create_tables([OnChainLog])
