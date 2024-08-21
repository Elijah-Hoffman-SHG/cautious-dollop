import sys
import redis
import pandas as pd
import json
from datetime import datetime, timedelta, time
from email_sender import EmailRow,EmailSection
from fetchers import FetcherBase

class RedisFetch(FetcherBase):
    def __init__(self, key_prefix):
        super().__init__()
        self.port = 9216
        self.db = 0
        self.key_prefix = key_prefix
        self.conns = [
            redis.Redis(host=self.lnk_hostname, port=self.port, db=self.db),
            redis.Redis(host=self.saw_hostname, port=self.port, db=self.db)
        ]
        self.max_exp = (60 * 60 * 24 * 30)
    
    def load_data(self, item_dict):
        now = datetime.now(tz='UTC')
        now_seconds = int(now.timestamp())
        df= pd.DataFrame(columns=['CRMID', 'bid_likelihood','exp', 'exp_in_redis', 'min_exp'])
        for conn in self.conns:
            pipe = conn.pipeline()
            for id, info in item_dict.items():
                exp = info['results']['exp'] - now_seconds
                exp_in_dict = info['results']['exp']
                liklihood = info['results']['bid_likelihood']

                if exp > 0:
                    df.loc[-1]={'CRMID': id, 'bid_likelihood':liklihood, 'exp':exp_in_dict, 'exp_in_redis':exp, 'min_exp':min(exp,self.max_exp)}
                    df.index = df.index + 1  
                    df = df.sort_index()
                    info['results'].pop('exp', None)
                    
                    pipe.set(f'{self.key_prefix}:{id}',json.dumps(info['results']), ex=min(exp,self.max_exp))

            pipe.execute()
        df.to_csv("aaRedisLoadCSVFIle.csv")

    def get_data(self, item_id):
        key = f'{self.key_prefix}:{item_id}'
        for conn in self.conns:
            data = conn.get(key)
            if data:
                return json.loads(data)
        return None
    
    def get_bid_likelihood(self, item_id):
        data = self.get_data(item_id)
        if data and 'bid_likelihood' in data:
            return data['bid_likelihood']
        return None
    def get_data_count(self, key="*"):
        res = {}
        conn_names = [self.lnk_hostname, self.saw_hostname]
        for i, conn in enumerate(self.conns):
            res.setdefault(conn_names[i], {})
            print('Starting Redis Count')
            start_time = datetime.now()
         
            iterat = conn.scan_iter(match=f"{self.key_prefix}{key}",count=10000)
            keys = list(iterat)
            email_keys_list = [key for key in keys if '@' in key.decode('utf-8')]
            #! todo maybe push to an array so the result wil be 
            # 'neo4j://siml105:7687': [44, 20909],
            #'neo4j://wiml105:7687': [44, 20909]
            res[conn_names[i]]['Email'] =len(email_keys_list)
            res[conn_names[i]]['CRMID'] = len(keys) - len(email_keys_list)
            res[conn_names[i]]['Total'] = len(keys)
            print(len(email_keys_list))
            print(f"Took {datetime.now() - start_time}")
            print(len(keys))
        return res
    
    def split_email_crmid(self, keys:list):
        email_keys_list = [key for key in keys if '@' in key.decode('utf-8')]
        return    
    
    def scan_iter_bench(self):
        for conn in self.conns:
            iterat = conn.scan_iter(match=f"{self.key_prefix}*",count=10000)
            keys = list(iterat)
        return 
    
    def keys_bench(self):
        for conn in self.conns:
            swag = conn.keys()
    def get_bench(self):
        for conn in self.conns:
            b= conn.get(self.key_prefix)
            print(b)
        return
        



