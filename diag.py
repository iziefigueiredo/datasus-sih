# %%
import pandas as pd

# %%
# ajuste o caminho se necessário
caminho = "data/support/municipios.csv"

df = pd.read_csv(caminho, dtype=str)

# %%
print("total de linhas:", len(df))
print("colunas:", list(df.columns))

# %%
vazio = df["codigo_6d"].isna() | (df["codigo_6d"].astype(str).str.strip() == "")

# %%
print("linhas com codigo_6d vazio/nulo:", vazio.sum())
print(df[vazio])