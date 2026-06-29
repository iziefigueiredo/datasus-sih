import ipeadatapy as ip

for code in ["AVS_IVS", "PNADCA_TXETOTUF"]:
    print(f"\n=== {code} ===")
    df = ip.timeseries(code)
    print(f"Colunas: {df.columns.tolist()}")
    print(f"Total registros: {len(df)}")
    print(f"Anos disponíveis: {sorted(df['YEAR'].unique().tolist())}")
    print(f"Registros por ano (primeiros 5 anos):")
    print(df.groupby('YEAR').size().head())