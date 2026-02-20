import mysql.connector
import pandas as pd

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="your_password"
)

cursor = conn.cursor()

cursor.execute("CREATE DATABASE IF NOT EXISTS movie_analysis")
cursor.execute("USE movie_analysis")

cursor.execute("""
CREATE TABLE IF NOT EXISTS movies (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255),
    budget BIGINT,
    revenue BIGINT,
    vote_average FLOAT,
    genre VARCHAR(100)
)
""")

conn.commit()

df = pd.read_csv("movies.csv")
df = df[["title", "budget", "revenue", "vote_average", "genre"]]
df.dropna(inplace=True)

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO movies (title, budget, revenue, vote_average, genre)
        VALUES (%s, %s, %s, %s, %s)
    """, tuple(row))

conn.commit()

query = "SELECT * FROM movies"
df = pd.read_sql(query, conn)

df = df[(df["budget"] > 0) & (df["revenue"] > 0)]
df["success"] = df["revenue"] > df["budget"]

print("\nTotal Movies:", len(df))
print("Success Rate:", round(df["success"].mean() * 100, 2), "%")
print("\nAverage Budget:", df["budget"].mean())
print("Average Revenue:", df["revenue"].mean())
print("Average Rating:", df["vote_average"].mean())

print("\nRevenue by Genre:")
print(df.groupby("genre")["revenue"].mean().sort_values(ascending=False))

cursor.close()
conn.close()
