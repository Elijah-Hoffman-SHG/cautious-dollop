import os
import pandas as pd
import json
import datetime as dt
import pytz
from datetime import datetime, timedelta, time
from fetchers import FetcherBase


class FileFetch(FetcherBase):
    def __init__(self):
        super().__init__()
        self.retail_name = 'ActiveRetailListing.csv'
        self.auction_name = 'ATEFActiveListings.csv'
        self.failed_listings_file_name = 'failed_listings.csv'
        self.failed_users_file_name = 'failed_users.csv'
    
    def get_new_listings(self, path):
        retail_file_path = os.path.join(path, self.retail_name)
        auction_file_path = os.path.join(path, self.auction_name)
        filter_date = (dt.datetime.now(pytz.timezone('UTC')) - dt.timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0).strftime('%Y-%m-%d %H:%M:%S')


        retail_df = pd.read_csv(retail_file_path, iterator=True, chunksize=1000)
        df = pd.concat([chunk[(chunk['ListingCreatedOnUTCDateTime'] > filter_date)|(chunk['ListingModifiedOnUTCDateTime']>filter_date)] for chunk in retail_df])

        auction_df = pd.read_csv(auction_file_path, iterator=True, chunksize=1000)
        df2 =pd.concat([chunk[(chunk['ListingCreatedOnUTCDateTime'] > filter_date)|(chunk['ListingModifiedOnUTCDateTime']>filter_date)] for chunk in auction_df])
 

        return df.shape[0], df2.shape[0]

    def get_failed_users(self, path):
        file_path = os.path.join(path, self.failed_users_file_name)
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            return df.shape[0]
        else:
            print(f"File not found: {file_path}")
            return 0
        
    def get_failed_listings(self, path):
        file_path = os.path.join(path, self.failed_listings_file_name)
        if os.path.isfile(file_path):
            df = pd.read_csv(file_path)
            reason_counts = df['reason'].value_counts()
            counts_dict = {
                'Failure Type:': 'Count:',
                'Total':df.shape[0]}
            counts_dict.update(reason_counts.to_dict())
            print(counts_dict)
            return counts_dict
        else:
            print(f"File not found: {file_path}")
            return 0
    