import os
import sqlite3 as sqlite

conn = sqlite.connect("project_db.db")

cur = conn.cursor()

queries_dict = {
    "drop_news": """DROP TABLE IF EXISTS news""",
    "create_news": """
    CREATE TABLE news (
    newsId INTEGER(3),
    newsTitle VARCHAR(173),
    newsText TEXT,
    CONSTRAINT pk_news PRIMARY KEY (newsId)
    )
    """,
    "drop_label": "DROP TABLE IF EXISTS label",
    "create_label": """
    CREATE TABLE label (
    newsId INTEGER(3),
    label BOOLEAN,
    CONSTRAINT pk_label PRIMARY KEY (newsId),
    CONSTRAINT fk_label_news FOREIGN KEY (newsId) 
                             REFERENCES news(newsId)
    )""",
    "drop_users": "DROP TABLE IF EXISTS users",
    "create_users": """
    CREATE TABLE users (
    userId INTEGER(5),
    CONSTRAINT pk_users PRIMARY KEY (userId)
    )""",
    "drop_followers": "DROP TABLE IF EXISTS followers",
    "create_followers": """
    CREATE TABLE followers (
    userId INTEGER(5),
    userId_followed INTEGER(5),
    CONSTRAINT pk_followers PRIMARY KEY (userId, userId_followed),
    CONSTRAINT fk_followers_users FOREIGN KEY (userId) REFERENCES users(userId),
    CONSTRAINT fk_following_users FOREIGN KEY (userId_followed) REFERENCES users(userId)
    )
    """,
    "drop_propagation": "DROP TABLE IF EXISTS propagation",
    "create_propagation": """
    CREATE TABLE propagation (
    newsId INTEGER(3),
    userId INTEGER(5),
    propaCount INTEGER(2),
    CONSTRAINT pk_propagation PRIMARY KEY (newsId, userId),
    CONSTRAINT fk_propagation_news FOREIGN KEY (newsId) REFERENCES news(newsId),
    CONSTRAINT fk_propagation_users FOREIGN KEY (userId) REFERENCES users(userId) 
    )"""
}

# Run all queries
for action, query in queries_dict.items():
    print(action)
    cur.execute(query)

# Close the connection
conn.close()