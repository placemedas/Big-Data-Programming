import sys
import re
import os
import gzip
import datetime
import uuid
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

def main(input_dir, userid, table):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    insert_log = session.prepare('INSERT INTO ' +  table + ' (host,datetime,path,bytes,id) values (?,?,?,?,?)')
    batch = BatchStatement()
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            limit = 200
            iter = 0
            for line in logfile:
                log_struct = line_re.findall(line)
                if log_struct != []:
                    host = log_struct[0][0]
                    path = log_struct[0][2]
                    date = datetime.datetime.strptime(log_struct[0][1],'%d/%b/%Y:%H:%M:%S')
                    bytes = int(log_struct[0][3])
                    uui = uuid.uuid4()
                    batch.add(insert_log, (host,date,path,bytes,uui))
                    iter = iter + 1
                    if iter >= limit:
                       session.execute(batch)
                       batch = BatchStatement()
                       iter = 0
            session.execute(batch)


if __name__ == '__main__':
    cluster = Cluster(['199.60.17.32', '199.60.17.65'])
    input_dir = sys.argv[1]
    userid = sys.argv[2]
    table  = sys.argv[3]
    session = cluster.connect(userid)
    main(input_dir,userid,table)

