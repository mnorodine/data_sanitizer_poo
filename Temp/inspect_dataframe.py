import yfinance as yf
import pandas as pd

# Choisir un ticker simple pour test (ex : Apple)
#ticker = "AAPL"
ticker = "BNP.PA"

# Télécharger les données à partir d'une date fixe
df = yf.download(ticker, start="2022-01-01", progress=False)
if isinstance(df.columns, pd.MultiIndex):
    df.columns = df.columns.droplevel(1)


# Inspection générale
print("\n🧾 Aperçu brut du DataFrame :")
print(df.head(5))

print("\n📐 Dimensions (lignes, colonnes) :", df.shape)

print("\n🔠 Noms des colonnes :")
print(df.columns)

print("\n📅 Type et contenu de l'index :")
print(type(df.index))
print(df.index)

print("\n🔍 Types de données par colonne :")
print(df.dtypes)

print("\n📊 Statistiques descriptives :")
print(df.describe())

print("\n📍 Première ligne (df.iloc[0]) :")
print(df.iloc[0])
