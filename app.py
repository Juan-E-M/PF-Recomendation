from flask import Flask, request, jsonify
import dask.dataframe as dd
import pandas as pd
import numpy as np
import os

app = Flask(__name__)

def movie_lens_to_binary(input_file, output_file, start_user_id=1, end_user_id=34500):
    ratings = dd.read_csv(input_file, sep=',', dtype={'userId': 'int32', 'movieId': 'int32', 'rating': 'float32'})
    filtered_ratings = ratings[(ratings['userId'] >= start_user_id) & (ratings['userId'] <= end_user_id)]
    selected_columns = ['userId', 'movieId', 'rating']
    selected_data = filtered_ratings[selected_columns].compute().to_numpy().astype(np.float32)
    with open(output_file, "wb") as bin_file:
        bin_file.write(selected_data.tobytes())


file_name = 'ratings.bin'
if os.path.isfile(file_name):
    print(f'Ratings.bin ready')
else:
    movie_lens_to_binary('ratings.csv', 'ratings.bin')
    print('converted to bin')


def binary_to_pandas_with_stats(bin_file):
    with open(bin_file, 'rb') as f:
        binary_data = f.read()
    np_data = np.frombuffer(binary_data, dtype=np.float32).reshape(-1, 3)
    df = dd.from_pandas(pd.DataFrame(np_data, columns=['userId', 'movieId', 'rating']), npartitions=1)
    return df.compute() #to use pandas

def consolidate_data(df):
    consolidated_df = df.groupby(['userId', 'movieId'])['rating'].mean().to_frame().unstack()
    return consolidated_df

df = binary_to_pandas_with_stats('ratings.bin')
consolidated_df = consolidate_data(df)
print(consolidated_df)


def limpia(np1, np2):
    mask = ~np.isnan(np2)
    np1 = np1[mask]
    np2 = np2[mask]
    np1, np2 = np2, np1
    mask = ~np.isnan(np2)
    np1 = np1[mask]
    np2 = np2[mask]
    np1, np2 = np2, np1
    return pd.DataFrame({'A': np1, 'B': np2})
def computeManhattanDistance(ax, bx):
    return np.sum(np.abs(ax - bx))
def computeNearestNeighbor(username, users_df):
    user_data = np.array(users_df.loc[username])
    distances = np.empty((users_df.shape[0],), dtype=float)
    for i, (index, row) in enumerate(users_df.iterrows()):
        if index != username:
            ax = np.array(row)
            bx = np.array(user_data)
            temp = limpia(ax, bx)
            ax = np.array(temp["A"].tolist())
            bx = np.array(temp["B"].tolist())
            distances[i] = computeManhattanDistance(ax, bx)
    sorted_indices = np.argsort(distances)
    sorted_distances = distances[sorted_indices]
    return list(zip(sorted_distances, users_df.index[sorted_indices]))
# def get_movie_recommendations(username, users_df, nearest_neighbors, movies_df, num_recommendations=10):
#     user_data = np.array(users_df.loc[username])
#     recommendations = {}
#     for neighbor_distance, neighbor_id in nearest_neighbors:
#         if neighbor_id == username:
#             continue
#         neighbor_data = np.array(users_df.loc[neighbor_id])
#         distance = computeManhattanDistance(user_data, neighbor_data)
#         if distance == 0:
#             continue
#         neighbor_ratings = users_df.loc[neighbor_id]
#         user_unrated_movies = np.isnan(user_data) & ~np.isnan(neighbor_ratings)

#         for movie_id, rating in enumerate(neighbor_ratings):
#             if user_unrated_movies[movie_id]:
#                 if movie_id not in recommendations:
#                     recommendations[movie_id] = (rating * neighbor_distance)
#                 else:
#                     recommendations[movie_id] += (rating * neighbor_distance)
#     sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
#     movie_ids = [movie_id for movie_id, _ in sorted_recommendations[:num_recommendations]]
#     return movie_ids

# user_id_to_recommend = 1
# nearest_neighbors = computeNearestNeighbor(user_id_to_recommend, consolidated_df)
# recommended_movie_ids = get_movie_recommendations(user_id_to_recommend, consolidated_df, nearest_neighbors, consolidated_df.columns)
# print(recommended_movie_ids)



def get_movie_recommendations(username, users_df, nearest_neighbors, num_recommendations=10):
    user_data = np.array(users_df.loc[username])
    recommendations = {}
    for neighbor_distance, neighbor_id in nearest_neighbors:
        if neighbor_id == username:
            continue
        neighbor_data = np.array(users_df.loc[neighbor_id])
        distance = computeManhattanDistance(user_data, neighbor_data)
        if distance == 0:
            continue
        user_unrated_movies = np.isnan(user_data) & ~np.isnan(neighbor_data)
        unrated_movie_indices = np.where(user_unrated_movies)[0]
        for movie_id in unrated_movie_indices:
            rating = neighbor_data[movie_id]
            if movie_id not in recommendations:
                recommendations[movie_id] = (rating * neighbor_distance)
            else:
                recommendations[movie_id] += (rating * neighbor_distance)
    sorted_recommendations = sorted(recommendations.items(), key=lambda x: x[1], reverse=True)
    movie_ids = [movie_id for movie_id, _ in sorted_recommendations[:num_recommendations]]
    return movie_ids


@app.route("/", methods=['GET'])
def hello():
    user_id_to_recommend = 1
    nearest_neighbors = computeNearestNeighbor(user_id_to_recommend, consolidated_df)
    recommended_movie_ids = get_movie_recommendations(user_id_to_recommend, consolidated_df, nearest_neighbors)
    recommended_movie_ids = [int(movie_id) for movie_id in recommended_movie_ids]                                                                                                                                           
    return jsonify({'mr': recommended_movie_ids, 'lenght':len(nearest_neighbors)})


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
