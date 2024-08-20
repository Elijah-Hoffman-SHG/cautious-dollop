from pydantic import ValidationError
from fetchers import FetcherBase
from qdrant_client import QdrantClient, models
import numpy as np
import json
import pandas as pd
import datetime as dt
from tqdm import tqdm
from typing import List, Dict

class QdrantFetch(FetcherBase):
    def __init__(self, collection, all_check=True):
        super().__init__()
        self.port = 9217
        self.clients = [
            QdrantClient(self.lnk_hostname, port=self.port, timeout=100000),
            QdrantClient(self.saw_hostname, port=self.port, timeout=100000)
        ]
        self.collection = collection
        self.all_check = all_check

    def update_placeholder(self, payloads: List[Dict], client: QdrantClient) -> None:
            '''
            Update the is_placeholder field in the collection for only certain points.

            `payloads` should be a list of dicts of the following format:
                [
                    {"is_placeholder": True,  "points": [1, 2, 3, ...]},
                    {"is_placeholder": False, "points": [4, 5, 6, ...]},
                    ...
                ]
            '''
            for payload in payloads:
                client.set_payload(
                    collection_name=self.collection,
                    payload={'is_placeholder': payload['is_placeholder']},
                    points=payload['points']
                )

    def get_point_count(self):
        client = QdrantClient(self.lnk_hostname, port=9217)
        try:
                count_result = client.count(
                    collection_name=self.collection,
                    exact=True,
                )
                return count_result.count
        except ValidationError as e:
                print(f"Validation error fetching collection from {client}: {e}")
        except Exception as e:
                print(f"Error fetching collection from {client}: {e}")
    
    @staticmethod
    def get_expired_users():
        client = QdrantClient('recsys.sandhills.int', port=9217)
        expired_results = list(
            map(
                dict,
                client.scroll(
                    collection_name="live_recsys_user",
                    scroll_filter=models.Filter(
                        must=[
                            models.FieldCondition(
                                key="Expiration",
                                range=models.Range(lt = (dt.datetime.now(dt.timezone.utc).replace(minute=0, hour=0, second=0, microsecond=0).timestamp())),
                            ),
                        ],
                    ),
                    limit=100_000,
                    with_payload=['Email'],
                    with_vectors=False,
                )[0]
            )
        )
        return [i['payload']['Email'] for i in expired_results]
    
    @staticmethod
    def get_null_placeholders(collection:str) -> pd.DataFrame:
        client = QdrantClient('recsys.sandhills.int', port=9217) 
        null_placeholders = client.scroll(
            collection_name=collection,
            scroll_filter=models.Filter(
                # Use is empty condition because it will match where a value "either does not exist, or has null or []"
                # As opposed to is null condition which will only match where a payload field exists and the value is null
                must=[models.IsEmptyCondition(is_empty=models.PayloadField(key='is_placeholder'))]
            ),
            limit=100_000,
            with_payload=['media_id'],
            with_vectors=False
        )[0]
        
        null_df = pd.json_normalize(list(map(dict, null_placeholders)))
        return null_df.rename(columns={'id': 'item_id', 'payload.media_id': 'media_id'})[['item_id', 'media_id']]

         
    
            