import inspect
from fetchers import FileFetch, QdrantFetch, Neo4jFetch, RedisFetch
from email_sender import EmailSection, EmailSender,EmailRow, EmailRowExp,EmailRowDict,EmailRowBase
#IF a Val is less than 256 it needs to be a float


def create_sections(*data_vars):
    sections = []
    current_frame = inspect.currentframe()
    caller_frame = inspect.getouterframes(current_frame)[1]
    local_vars = caller_frame.frame.f_locals

    for data in data_vars:
        for name, value in local_vars.items():
            if value is data:
                print('val is data, here it is')
                # if isinstance(data, list):
                #     for val2 in data:
                #         for name2,value2 in local_vars.items():
                #             if val2 is value2:
                #                 print("AER#R\n-----------DDIIING DING DING DING\n------- ")
                #                 print(name2)
                #                 print(val2)
                #                 print("AER#R\n-----------DDIIING DING DING DING\n------- ")
                # print(data)
                section2s = create_section(data, section_title=name)
                section = EmailSender().create_section_new(data, section_title=name)
                sections.append(section)
                sections.append(section2s)
                break

    return sections

def format_multi_dict(*dicts_with_titles):
    transformed_data = {}

    for dictionary, title in dicts_with_titles:
        for key, value in dictionary.items():
            # Extract the relevant part of the key
            new_key = key.split(':')[1].split('/')[2]  # Extract 'siml105' or 'wiml105'
            new_key = f"neo4j://{new_key}"
            
            # Initialize the nested dictionary if the new key is not already in transformed_data
            if new_key not in transformed_data:
                transformed_data[new_key] = {}
            
            # Add the value with the corresponding title
            transformed_data[new_key][title] = value

    return transformed_data

def process_dictionary(dict):
    print("-------+_____________---+")
    print("Welcome To The Fun Time")
    title_length = 0
    print('Here is this mug.')
    print(dict)
    print("And it has length of ")
    print(len(dict))
    #! Lengths of inner must match here
    lengths = [len(value) for value in dict.values()]
    if (len(set(lengths))!= 1):
        print("Error... miss match in length")
    else:
        title_length = lengths[0]
    print(f"We need to have {title_length} titles")

    titles = [val for val in dict.keys()]
    deep_titles = []
    print(titles)
    print(lengths)
    for outer_key, inner_dict in dict.items():
        #! names of the the things will be used as headers and must Match
        titles = [(value) for value in inner_dict.keys()]
        if (len(titles) != title_length):
            print(f"Error....... length of titles missmatched.")
        deep_titles.extend(titles)
    
    if(len(set(deep_titles))!=title_length):
        print(deep_titles)
        print("Error.... He Said Happy BirthDay How Does It Feel To Be 15. I Remarked I Have Been 15 For Many Years")
    else:
        print("We Made IT")
        fun_swag = titles
    return EmailRowDict(dict,titles)
         

    print("-------+_____________---+")

def create_section(list_of_vals_for_rows, name_str = '', section_title='error didnt work bucko'):

    current_frame = inspect.currentframe()
    caller_frame = inspect.getouterframes(current_frame)[1]
    local_vars = caller_frame.frame.f_locals
    
    # Find the variable name for the data
    variable_name = None
    for name, val in local_vars.items():
        if (name is name_str):
            print(val)
        if val is list_of_vals_for_rows:
            print(name)
            section_title = name.replace('_', ' ').title()
            break
        
    
    rows =[]
    for item in list_of_vals_for_rows:
        if isinstance(item, tuple):
            if len(item) == 3:
                value, headers, corner_header = item
            else:
                value, corner_header = item
            
        else:
            value = item
            headers = None
            corner_header = ''
        
        current_frame = inspect.currentframe()
        caller_frame = inspect.getouterframes(current_frame)[1]
        local_vars = caller_frame.frame.f_locals
        
        variable_name = None
        for name, val in local_vars.items():
            if val is value:
                variable_name = name
                break
        
        if variable_name:
            
            formatted_name = variable_name.replace('_', ' ').title()
            if(isinstance(value, dict)):
                rows.append(EmailRowDict(value, formatted_name,corner_header))
            else:
                rows.append(EmailRow(value, headers, title = formatted_name, corner_header=corner_header))
            
    return EmailSection(section_title, rows)
        

def dict_to_emails(dict):
    print(dict)

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

    print("Getting data from files")
    ff = FileFetch()
    retail_listings_modifed, auction_listings_modifed = FileFetch().get_new_listings(active_listing_path)
    failed_bid_propensity_emails_count = ff.get_failed_users(data_file_loc)

    failed_listings_count = ff.get_failed_listings(data_file_loc)


    print("Getting data from Qdrant")
    activity_point_count = QdrantFetch(qdrant_activity).get_point_count()
    similar_items_point_count = QdrantFetch(qdrant_similar_items).get_point_count()
    qdrant_user_count = QdrantFetch(qdrant_user).get_point_count()

    print("Getting data from Neo4j")
    added_user_count = Neo4jFetch().get_users_count_by_date("CreatedAt")
    refreshed_user_count = Neo4jFetch().get_users_count_by_date("RefreshedAt")
    users_updated_neo4j = format_multi_dict(
        (added_user_count, "Added Users"),
        (refreshed_user_count, "Refreshed Users"))


    print("Getting data from redis")
    #redis_bid_prep_count= RedisFetch(redis_prefix).get_data_count()
    redis_bid_prep_count = {'siml4': {'Email': 2257, 'CRMID': 1212, 'Total': 6679}, 'wiml4': {'Email': 2257, 'CRMID':32131, 'Total': 6679}}

    print(f"Retail listings modified: {retail_listings_modifed}")
    print(f"Auction listings modified: {auction_listings_modifed}")

    print(f"Count of points in activity {activity_point_count}")
    print(f"Count of points in similar items {similar_items_point_count}")

    print(f"Count of users added today in Neo4j {added_user_count}")
    print(f"Count of users refreshed today in Neo4j {refreshed_user_count}")

   
    neo4j_data = [(users_updated_neo4j)]

    file_dump_data = [(failed_listings_count, "Failure Type"), failed_bid_propensity_emails_count, retail_listings_modifed, auction_listings_modifed]

    redis_data = [(redis_bid_prep_count, "Key Type")]

    qdrant_data = [activity_point_count, similar_items_point_count, qdrant_user_count]

    section_n = create_section(neo4j_data)
    section23 = create_section(redis_data)
    section_q = create_section(qdrant_data)
    section_f = create_section(file_dump_data)

    hmmm = create_sections(neo4j_data,redis_data,qdrant_data,file_dump_data)
    print("Have We CreateD The Sections")
    print(hmmm)
    for i in hmmm:
        print(hmmm)
    EmailSender().send_emails([section_n,section23,section_q,section_f])
    EmailSender().send_emails(hmmm)


if __name__=="__main__":
    main()