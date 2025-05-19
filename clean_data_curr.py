import pandas as pd

def process():
    ratings_path = "project/ratings.csv"
    movies_metadata_path = "project/movies_metadata.csv"
    links_path = "project/links.csv"
    
    # Load CSV files into pandas DataFrames
    ratings_df = pd.read_csv(ratings_path)
    movies_metadata_df = pd.read_csv(movies_metadata_path, low_memory=False)
    links_df = pd.read_csv(links_path)

    # Convert imdb_id in movies_metadata.csv to numeric (remove 'tt' and convert to float)
    movies_metadata_df['imdb_id'] = movies_metadata_df['imdb_id'].apply(lambda x: str(x).replace('tt', '')).astype(float)

    # Remove rows with null values in 'movieId' column of ratings_df
    ratings_df = ratings_df.dropna(subset=['movieId'])

    # Remove rows with null values in 'id' and 'imdb_id' columns of movies_metadata_df
    movies_metadata_df = movies_metadata_df.dropna(subset=['id', 'imdb_id'])

    # Remove rows with null values in 'imdbId' column of links_df
    links_df = links_df.dropna(subset=['imdbId'])

    # Merge using 'imdb_id' from movies_metadata_df and 'imdbId' from links_df
    movies_df = pd.merge(movies_metadata_df, links_df[['imdbId']], left_on='imdb_id', right_on='imdbId', how='left')

    # Convert both 'id' and 'movieId' columns to numeric types (int64)
    movies_df['id'] = pd.to_numeric(movies_df['id'], errors='coerce')
    ratings_df['movieId'] = pd.to_numeric(ratings_df['movieId'], errors='coerce')

    # Merge ratings_df with movies_df based on 'movieId', include 'userId' in the merge
    movies_df = pd.merge(movies_df, ratings_df[['movieId', 'rating', 'userId']], left_on='id', right_on='movieId', how='left')

    total_movies_count = movies_df.shape[0]

    # Remove rows where 'movieId' value does not match 'id' value
    movies_df = movies_df.dropna(subset=['movieId'])
    movies_df = movies_df[movies_df['movieId'] == movies_df['id']]
    movies_with_id_match_count = movies_df.shape[0]

    # Select the columns to be written to the output file, including 'userId'
    selected_columns = ['title', 'movieId', 'imdbId', 'rating', 'userId', 'genres']

    # Write the result to a CSV file
    output_file_path = "project/movies.txt"  # Change the path as needed
    # Write the result to a CSV file with tab delimiter

    movies_df[selected_columns].drop_duplicates().to_csv(output_file_path, sep='\t', index=False)


# Call the function
process()
