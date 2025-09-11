from pathlib import Path
import polars as pl



class UnifyReport:
    def __init__(self, df: pl.DataFrame, critical_cols=None):
        self.df = df
        self.total_rows = df.height
        self.critical_cols = critical_cols or ["N_AIH", "CNES", "PROC_REA", "DT_INTER"]

    def check_completeness(self):
        """
        Calculate null-rate for each column.
        Returns a list of dicts with {column, non_null, null, null_rate, critical}.
        """
        results = []
        for col in self.df.columns:
            nulls = self.df[col].null_count()
            non_null = self.total_rows - nulls
            rate = (nulls / self.total_rows) if self.total_rows > 0 else 0.0
            results.append({
                "column": col,
                "non_null": non_null,
                "null": nulls,
                "null_rate": round(rate, 6),
                "critical": col in self.critical_cols
            })
        return results

    def export_completeness(self, results, outdir="reports/unify"):
        outdir = Path(outdir)
        outdir.mkdir(parents=True, exist_ok=True)
        outfile = outdir / "completeness.csv"
        pl.DataFrame(results).write_csv(outfile)
        print(f"[INFO] Completeness exportado para {outfile}")    



def main():
    parquet_path = Path("data/parquet_unified/sih_rs.parquet")
    if not parquet_path.exists():
        print("[WARNING] Arquivo não encontrado:", parquet_path)
        return

    df = pl.read_parquet(parquet_path)
    report = UnifyReport(df)
    results = report.check_completeness()
    report.export_completeness(results)

   


if __name__ == "__main__":
    main()