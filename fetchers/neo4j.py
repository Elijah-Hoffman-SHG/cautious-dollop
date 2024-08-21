import pandas as pd
from py_framework.common.sandhills_config import Config
from py_framework.bis.connectors import Neo4jConnector
        
class Neo4jFetch:

    def __init__(self):
        """
        Initializes a loader to load Embedding Data into Neo4j Servers
        """
        
        self.lnk_connection = Config().get('con_neo4j_LNK', config_section='default')
        self.saw_connection = Config().get('con_neo4j_SAW', config_section='default')



    def get_users_count_by_date(self, date_field):
        if date_field not in ["CreatedAt", "RefreshedAt"]:
            raise ValueError("Invalid date field. Must be 'CreatedAt' or 'RefreshedAt'.")

        query = f"""
        MATCH (u:User)
        WHERE date(u.{date_field}) = date()
        RETURN count(u) as total
        """
        with Neo4jConnector([self.lnk_connection, self.saw_connection]) as neo4j:
            res = neo4j.query(query=query)
      
            if res and res[0] and res[0][0]:

                return {
                    self.lnk_connection.get('uri') : res[0][0]['total'],
                    self.saw_connection.get('uri') : res[1][0]['total'],
                }
            else:
                raise Exception(f"Error querying data using {query}")

    def get_added_users(self):
        return self.get_users_count_by_date("CreatedAt")

    def get_refreshed_users(self):
        return self.get_users_count_by_date("RefreshedAt")
