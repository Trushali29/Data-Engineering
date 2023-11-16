import pandas as pd
import numpy as np

store = pd.read_excel("SuperStoreUS.xlsx")
print("View dataset: \n",store.head())

print("\n view columns:\n",store.columns)

print("\nDescribe dataset: \n",store.describe())

print("\nStore null values : \n",store.isna().sum())

store = store.dropna()
print("\nClean dataset: \n",store.isna().sum())

print("\nShape of the dataset: ",store.shape)

print("\nSave the clean dataset: ",store.to_excel("SuperStore.xlsx"))
