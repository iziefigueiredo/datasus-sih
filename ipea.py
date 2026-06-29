import ipeadatapy as ipea

meta = ipea.metadata()
anuais = meta[meta["FREQUENCY"] == "Anual"].copy()

temas_interesse = anuais[anuais["BIG THEME"].isin(["Regional", "Social"])]

temas_interesse[["CODE", "NAME", "SOURCE ACRONYM", "MEASURE", "THEME CODE", "BIG THEME"]].to_csv("ipea_series.csv", index=False)
print(f"{len(temas_interesse)} séries salvas em ipea_series.csv")