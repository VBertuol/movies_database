# %%

import pandas as pd
import gc
from statistics import mean 

import_path = '../../data/my/my.csv'
save_path = '../../data/my/my_means.csv'
# %%
df = pd.read_csv(import_path)
# %%
years_stats = df.groupby('startYear').agg(
    personal_score_mean=('personal_score', 'mean'),  # Média de personal_score
).reset_index()

years_stats['decade'] = (years_stats['startYear'] // 10) * 10

decades_stats = years_stats.groupby('decade').agg({
    'personal_score_mean': 'mean'
}).reset_index()

df['decade'] = (df['startYear'] // 10) * 10

# Criar um dicionário para mapear 'decade' para 'personal_score_mean'
mapping = dict(zip(decades_stats['decade'], decades_stats['personal_score_mean']))

# Substituir os valores em df['decade'] pelos valores correspondentes
df['decade'] = df['decade'].map(mapping)

df.drop(columns='startYear', inplace=True)
del years_stats
del decades_stats
gc.collect()

# %%
# Criar os intervalos
bins = [60, 90, 120, 150, 180, float('inf')]
labels = ['60-90', '91-120', '121-150', '151-180', '>180']

# Aplicar os intervalos na coluna 'runtimeMinutes'
df['runtimeInterval'] = pd.cut(df['runtimeMinutes'], bins=bins, labels=labels, right=False)
runtimeInterval_stats = df.groupby('runtimeInterval').agg(
    personal_score_mean=('personal_score', 'mean')  # Média de personal_score
).reset_index()

mapping = dict(zip(runtimeInterval_stats['runtimeInterval'], runtimeInterval_stats['personal_score_mean']))
df['runtimeInterval'] = df['runtimeInterval'].map(mapping)

df.drop(columns='runtimeMinutes', inplace=True)
del runtimeInterval_stats
gc.collect()

# %%
bins = [100, 1000, 10000, 100000, 1000000, float('inf')]
labels = ['100-1000', '1001-10000', '10001-100000', '100001-10000000', '>10000000']

# Aplicar os intervalos na coluna 'runtimeMinutes'
df['numVotesInterval'] = pd.cut(df['numVotes'], bins=bins, labels=labels, right=False)
numVotesInterval_stats = df.groupby('numVotesInterval').agg(
    personal_score_mean=('personal_score', 'mean')  # Média de personal_score
).reset_index()

mapping = dict(zip(numVotesInterval_stats['numVotesInterval'], numVotesInterval_stats['personal_score_mean']))
df['numVotesInterval'] = df['numVotesInterval'].map(mapping)

df.drop(columns='numVotes', inplace=True)
del numVotesInterval_stats
gc.collect()

# %%
# Passo 1: Explodir os writers em linhas separadas
df['writers'] = df['writers'].str.split(', ')  # Divide os writers
writers_exploded = df.explode('writers')  # Cria um df com uma linha por writer

# Passo 2: Agrupar por writer e calcular a média
writers_stats = writers_exploded.groupby('writers').agg(
    personal_score_mean=('personal_score', 'mean'),  # Média de personal_score
).reset_index()

mapping = dict(zip(writers_stats['writers'], writers_stats['personal_score_mean']))
df['writers'] = df['writers'].apply(lambda writer_list: mean([mapping.get(writer, None) for writer in writer_list]))


# %%
# Passo 1: Explodir os genres em linhas separadas
df['genres'] = df['genres'].str.split(', ')  # Divide os genres
genres_exploded = df.explode('genres')  # Cria um df com uma linha por genre

# Passo 2: Agrupar por write e calcular a média
genres_stats = genres_exploded.groupby('genres').agg(
    personal_score_mean=('personal_score', 'mean'),  # Média de personal_score
).reset_index()

mapping = dict(zip(genres_stats['genres'], genres_stats['personal_score_mean']))
df['genres'] = df['genres'].apply(lambda genre_list: mean([mapping.get(genre, None) for genre in genre_list]))

# %%
# Passo 1: Explodir os directors em linhas separadas
df['directors'] = df['directors'].str.split(', ')  # Divide os directors
directors_exploded = df.explode('directors')  # Cria um df com uma linha por director

# Passo 2: Agrupar por write e calcular a média
directors_stats = directors_exploded.groupby('directors').agg(
    personal_score_mean=('personal_score', 'mean'),  # Média de personal_score
).reset_index()

mapping = dict(zip(directors_stats['directors'], directors_stats['personal_score_mean']))
df['directors'] = df['directors'].apply(lambda director_list: mean([mapping.get(director, None) for director in director_list]))

# %%
# Passo 1: Explodir os nconst em linhas separadas
df['nconst'] = df['nconst'].str.split(', ')  # Divide os nconst
nconst_exploded = df.explode('nconst')  # Cria um df com uma linha por nconst

# Passo 2: Agrupar por write e calcular a média
nconst_stats = nconst_exploded.groupby('nconst').agg(
    personal_score_mean=('personal_score', 'mean'),  # Média de personal_score
).reset_index()

mapping = dict(zip(nconst_stats['nconst'], nconst_stats['personal_score_mean']))
df['nconst'] = df['nconst'].apply(lambda nconst_list: mean([mapping.get(nconst, None) for nconst in nconst_list]))

# %%
df.to_csv(save_path)
# %%
