import csv
import re
from threading import Thread
import time
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='db_bench.log',
                    filemode='a'
                    )

# dbconnect class
class postgresql:
    columns=None
    def query(self,sql):
        print (sql, file=sys.stderr)
        self.db.execute(sql)
        #res = format_result(self.db.fetchall())
        res = self.db.fetchall()
        self.columns=[i.name for i in self.db.description]
        return res
    def rowdict(self,res):
        # 返回 list(dict) 带有column 的行结果.[ {col1:val, col2:val},{col1:val,..},{...}...]
        return [dict(zip(self.columns,row)) for  row  in res ]

    def execute(self,sql):
        print (sql, file=sys.stderr)
        self.db.execute(sql)

    def executemany(self, sql,parm):
        n = self.db.executemany(sql,parm)
        return n

    def query_title(self,sql):
        print(sql)
        self.db.execute(sql)
        res = self.db.fetchall()
        title = [i[0] for i in self.db.description ]
        return res,title

    def close(self):
        self.db.close()
        self.conn.close()

    def __init__(self,host,port,dbname,user,password):
        import psycopg2
        ##用大串更灵活，psycopg2.connetc("host='xxxx' dbname='xxxx' user='xxx'")，可以不用五个参数全写，这里考虑到与mysql oracle 类的风格一致，采用如下方法
        self.conn = psycopg2.connect(host=host,port=port,dbname=dbname,user=user,password=password)
        self.db = self.conn.cursor()


# read csv file
def read_csv(filename):
    result = []
    with open(filename, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        for row in reader:
            sql=""
            parameters=""
            #print(row)
            if row[7] == "SELECT" and row[11] == 'LOG':
                if 'execute <unnamed>:' in row[13]:
                    sql = row[13].split('execute <unnamed>:')[1]
                if 'parameters:' in row[14]:
                    parameters = row[14].split('parameters:')[1]
                
                if sql:
                    result.append({"db":row[1],"user":row[2],"sql":sql,"parameters":parameters})
    logging.info("read csv file success")
    return result

# parse sql parameter
def parse_parameters(parameters_str):
    # 使用正则表达式解析 parameters 字符串
    param_pattern = re.compile(r'\$(\d+) = ([^,]+)',)
    params = param_pattern.findall(parameters_str)
    parlist=[]
    for num, val in params:
        parlist.insert(0,{f'${int(num)}': val.strip()})
    return parlist               

#format sql ,combine parameter value to sql
def format_sql(sql,parameters):
    # 解析 parameters 字符串
    param_list = parse_parameters(parameters)
    # 替换 sql 字符串中的占位符
    for eml in param_list:
        for key, value in eml.items():
            sql = sql.replace(key, value)
    return sql

# write log to textfile for each db
def write_to_file(filename,text):
    with open(filename,'a',encoding='utf8') as f:
        f.write("%s\n"%text)

# read csv log and then split sqls into each db
def split_sqlset():
    sqlset = {}
    data = read_csv('postgresql-2025-04-14.csv')
    for entry in  data:
        db = entry["db"]
        formatted_sql = format_sql(entry["sql"], entry["parameters"])
        if db in sqlset:
            sqlset[db].append(formatted_sql)
        else:
            sqlset[db] = [formatted_sql,]
    return sqlset

## set db password here
def get_dbpwd(db):
    dbset={"db1":"password1",
           "db2":"password2",
           "db3":"password3",
           "db4":"password4",
           "db5":"password5",
           "db6":"password6",
           }
    return dbset[db]

# replay sql with one db one thread
def thread_run_sql(db,sqlset):
    sqlnum = len(sqlset)
    write_to_file("%s-query.log"%db,"I AM THREAD %s will run %s sqls:"%(db,sqlnum))
    pwd = get_dbpwd(db)
    try:
        # replace this into your own db
        dbconn = postgresql('ip',port,db,db,pwd)
        for sql in sqlset:
            try:
                res = dbconn.query(sql)
            except Exception as e:
                write_to_file("%s-query.log"%db,"sql:%s "%(sql))
                write_to_file("%s-query.log"%db,"thread %s error:%s "%(db,str(e)))
                dbconn.close()
                dbconn = postgresql('ip',port,db,db,pwd)
            time.sleep(0.1)
            sqlnum -= 1
            write_to_file("%s-query.log"%db,"THREAD %s  %s sqls remain :"%(db,sqlnum))
    except Exception as e:
        write_to_file("%s-query.log"%db,"thread %s error:%s "%(db,str(e)))

if __name__ == '__main__':
    dbsqlset = split_sqlset()
    threadList = []
    for db,sqlset in dbsqlset.items():
        t = Thread(target=thread_run_sql,args=(db,sqlset))
        t.start()  
        threadList.append(t)

    for i in threadList:
        t.jon()
        
