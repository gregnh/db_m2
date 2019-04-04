import os
import pandas as pd
import sqlite3 as sqlite

path_data = "./data/"
path_news_users = os.path.join(path_data, "newsUser.txt")
path_followers = os.path.join(path_data, "UserUser.txt")
path_news = os.path.join(path_data, "news")
path_news_training = os.path.join(path_news, "training")
path_news_test = os.path.join(path_news, "test")

# Create to db
con = sqlite.connect("project_db.db")
cur = con.cursor()

## Fill user table

user_user_df = pd.read_csv(path_followers, sep="\t", header=None)
user_user_df.rename({0: "follower", 1: "user_followed"}, axis=1, inplace=True)
user_user_df.head()


# Easy to get userId: get unique values for one of these two columns of the UserUser file
for user_id in user_user_df.user_followed.unique():
    cur.execute("INSERT INTO users VALUES({})".format(user_id))

## Fill followers table
# https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html  
# DataFrame.to_sql params:  
# if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’

#     How to behave if the table already exists.

#         fail: Raise a ValueError.
#         replace: Drop the table before inserting new values.
#         append: Insert new values to the existing table.

# index : bool, default True

#     Write DataFrame index as a column. Uses index_label as the column name in the table.

user_user_df.rename({"follower": "userId", "user_followed": "userId_followed"}, 
                    axis=1, 
                    inplace=True)
user_user_df.to_sql("followers", con=con, if_exists='append', index=False)

## Fill News and label tables
### News table

def read_news_file(path_file):
    text = ""
    with open(path_file, "r") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if i == 0:
                title = line
            if line:
                text += "{}\n".format(line)
    return (title, text)

news_dict = dict()
for file in os.listdir(path_news_training):
    news_id = int(os.path.splitext(file)[0])
    if news_id not in news_dict.keys():
        news_dict[news_id] = read_news_file(os.path.join(path_news_training, file))   
    
has_label_df = pd.read_csv(os.path.join(path_data, "labels_training.txt"), header=0)

# checking if the number of news having a label is matching 
# between the training folder and the label file
assert len(news_dict.keys()) == len(has_label_df)

for news_id, news_text in news_dict.items():
    cur.execute("""INSERT INTO news VALUES({}, "{}", "{}")""".format(news_id, 
                                                                     news_text[0].replace('"', '""'), 
                                                                     news_text[1].replace('"', '""')))
# explication pour le replace double quote : https://stackoverflow.com/questions/25387537/inserting-a-table-name-into-a-query-gives-sqlite3-operationalerror-near-sy

### Label table

label_df = pd.read_csv(os.path.join(path_data, "labels_training.txt"), header=0)
label_df.rename({"doc": "newsId", "class": "label"}, axis=1, inplace=True)
label_df.to_sql("label", con=con, if_exists='append', index=False)

pd.read_sql_query("SELECT * FROM label LIMIT 10", con=con)

## Propagation

news_user_df = pd.read_csv(path_news_users, sep="\t", header=None)
news_user_df.rename({0: "newsId", 1: "userId", 2: "propaCount"}, 
                    axis=1, 
                    inplace=True)
news_user_df.head()

news_user_df.to_sql("propagation", con=con, if_exists="append", index=False)


