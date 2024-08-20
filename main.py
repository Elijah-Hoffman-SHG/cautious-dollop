from fetchers import FileFetch, QdrantFetch, Neo4jFetch, RedisFetch
from email_sender import EmailSender
def main():
    env = 'dev'
    active_listing_path = f'/mnt/Machine_Learning/spark_data/dbd_files/{env}/active_listings'
    data_file_loc = '/mnt/Machine_Learning/spark_data/recsys_files/daily_refresh'

    qdrant_activity = 'recsys_activity'
    qdrant_similar_items = 'recsys_similar_items'
    qdrant_user = 'recsys_user'
    qdrant_activity = f'{env}_{qdrant_activity}'
    qdrant_similar_items = f'{env}_{qdrant_similar_items}'
    qdrant_user = f'{env}_{qdrant_user}'
    redis_prefix = f"{env}:bid"

    RedisFetch(redis_prefix).get_bench()
    print("Getting data from files")
    ff = FileFetch()
    #retail_listings_modifed, auction_listings_modifed = FileFetch().get_new_listings(active_listing_path)
    failed_bid_propensity_emails_count = ff.get_failed_users(data_file_loc)

    retail_listings_modifed = 0
    auction_listings_modifed = 0

    failed_listings_count = ff.get_failed_listings(data_file_loc)
    



    print("Getting data from Qdrant")
    activity_point_count = QdrantFetch(qdrant_activity).get_point_count()
    similar_items_point_count = QdrantFetch(qdrant_similar_items).get_point_count()
    qdrant_user_count = QdrantFetch(qdrant_user).get_point_count()

    print("Getting data from Neo4j")
    added_user_count = Neo4jFetch().get_users_count_by_date("CreatedAt")
    refreshed_user_count = Neo4jFetch().get_users_count_by_date("RefreshedAt")

    print("Getting data from redis")
    #redis_bid_prep_count= RedisFetch(redis_prefix).get_data_count()
    redis_bid_prep_count = 0

    print(f"Retail listings modified: {retail_listings_modifed}")
    print(f"Auction listings modified: {auction_listings_modifed}")

    print(f"Count of points in activity {activity_point_count}")
    print(f"Count of points in similar items {similar_items_point_count}")

    print(f"Count of users added today in Neo4j {added_user_count}")
    print(f"Count of users refreshed today in Neo4j {refreshed_user_count}")

    summary = {
        "File Dump Data" : {
        'Retail listings modified': retail_listings_modifed,
        "Auction listings modified": auction_listings_modifed,
        "Failed listings": failed_listings_count,
        "Count of bid propensity crmids that didnt have an email":failed_bid_propensity_emails_count,
        },
        "Neo4jData":{
        "Count of users added today in Neo4j":added_user_count,
        "Count of users refreshed today in Neo4j": refreshed_user_count,
        },
        "QDrant Data":{
        "Count of points in activity": activity_point_count,
        "Count of points in similar items": similar_items_point_count,
        "Count of points in user": qdrant_user_count,
        },
        "Redis Data":{
        f"Amount of Keys with {redis_prefix} prefix" :redis_bid_prep_count
        }
        

    }
    {'siml4': [1,3]}
    
    EmailSender().send_emails(summary)



if __name__=="__main__":
    main()