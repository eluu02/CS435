import pandas as pd
import random
# from google.colab import drive

# Mount Google Drive (only need to run this once)
#drive.mount('/content/drive')

# Set the Paths to the Files Stored in Google Drive
ratings_path = "/s/chopin/n/under/amanda95/CS435_gp/CS435-Team-2/ratings.csv"
movies_metadata_path = "/s/chopin/n/under/amanda95/CS435_gp/CS435-Team-2/movies_metadata.csv"
links_path = "/s/chopin/n/under/amanda95/CS435_gp/CS435-Team-2/links.csv"

# Load CSV files into pandas DataFrames
ratings_df = pd.read_csv(ratings_path)
movies_metadata_df = pd.read_csv(movies_metadata_path, low_memory=False)
links_df = pd.read_csv(links_path)
print(f"The column names of original ratings_df are: {ratings_df.columns}")
print(f"The shape of original ratings_df is: {ratings_df.shape}")

# Convert imdb_id in movies_metadata.csv to numeric (remove 'tt' and convert to float)
movies_metadata_df['imdb_id'] = movies_metadata_df['imdb_id'].apply(lambda x: str(x).replace('tt', '')).astype(float)

# Remove rows with null values in 'movieId' column of ratings_df
ratings_df = ratings_df.dropna(subset=['movieId'])
print(f"The column names of ratings_df with null values removed are: {ratings_df.columns}")
print(f"The shape of original ratings_df with null values removed is: {ratings_df.shape}")
# Remove rows with null values in 'id' and 'imdb_id' columns of movies_metadata_df
movies_metadata_df = movies_metadata_df.dropna(subset=['id', 'imdb_id'])

# Remove rows with null values in 'imdbId' column of links_df
links_df = links_df.dropna(subset=['imdbId'])

print(f"The original shape of movies_metadata_df (for comparison): {movies_metadata_df.shape}")

# Merge using 'imdb_id' from movies_metadata_df and 'imdbId' from links_df
movies_df = pd.merge(movies_metadata_df, links_df[['imdbId']], left_on='imdb_id', right_on='imdbId', how='left')
print(f"The column names of movies_df after adding 'imdbId' from links_df are: {movies_df.columns}")
print(f"The shape of movies_df after adding 'imdbId' from links_df is: {movies_df.shape}")

# Convert both 'id' and 'movieId' columns to numeric types (int64)
movies_df['id'] = pd.to_numeric(movies_df['id'], errors='coerce')
ratings_df['movieId'] = pd.to_numeric(ratings_df['movieId'], errors='coerce')

# add 'movieId' from ratings_df to movies_df
movies_df = pd.merge(movies_df, ratings_df[['movieId']], left_on='id', right_on='movieId', how='left')
print(f"The column names of movies_df after adding 'movieId' from ratings.csv are: {movies_df.columns}")
print(f"The shape of movies_df after adding 'movieId' from ratings.csv is: {movies_df.shape}")
total_movies_count = movies_df.shape[0]

# remove rows where 'movieId' value does not match 'id' value
movies_df = movies_df.dropna(subset=['movieId'])
movies_df = movies_df[movies_df['movieId'] == movies_df['id']]
print(f"The column names of movies_df after removing rows that don't have matching 'movieId' and 'id' value is: {movies_df.columns}")
print(f"The shape of movies_df after removing rows that don't have matching 'movieId' and 'id' value is: {movies_df.shape}")
movies_with_id_match_count = movies_df.shape[0]

# if 'movieId' matches 'id' value then add rating from ratings_df
# uncommenting the next line makes it take forever but we can deal with that later
#movies_df['rating'] = movies_df.apply(lambda row: ratings_df[ratings_df['movieId'] == row['id']]['rating'].values[0] if row['id'] in ratings_df['movieId'].values else None, axis=1)

print(f"The final shape of movies_df is: {movies_df.shape}")
# print the difference between movies_df size
movies_without_id_match_count = total_movies_count - movies_with_id_match_count
print(f"Total number of movies: {total_movies_count}")
print(f"Number of movies with matching 'movieId' and 'id' value: {movies_with_id_match_count}")
print(f"Number of movies without matching 'movieId' and 'id' value: {movies_without_id_match_count}")

# Display the first 100 rows with selected columns
print(movies_df[['title', 'movieId', 'id', 'imdb_id', 'imdbId']].drop_duplicates().head(100))

# Display the first 100 rows with selected columns
print(movies_df[['title', 'movieId', 'id', 'imdb_id', 'imdbId']].drop_duplicates().head(100))

# Save processed data to a file named movies.txt
# temporarily saving to local path 
output_path = "/s/chopin/n/under/amanda95/CS435_gp/CS435-Team-2/movies.txt"
movies_df[['title', 'movieId', 'id', 'imdb_id', 'imdbId']].drop_duplicates().to_csv(
    output_path, sep='\t', index=False, header=False
)
print(f"Processed data saved to {output_path}")
