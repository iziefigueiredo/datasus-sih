import ipeadatapy as ip
import time

for code in [ 'AVIOL12_ACIDT']:
    try:
        df = ip.timeseries(code)
        obs = df.groupby('YEAR').size()
        mediana = obs.median()
        ano_ref = obs.index[-2]
        print(f"{code}: {mediana:.0f} obs/ano (ex: {ano_ref}={obs[ano_ref]})")
        time.sleep(1)
    except Exception as e:
        print(f"{code}: ERRO — {e}")