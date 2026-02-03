import os
import sys
import pandas as pd
import numpy as np
import tomllib
from pathlib import Path
from dataclasses import dataclass
from typing import Dict, Any, List


@dataclass
class VnfConfig:
    input_file: str
    output_dir: str
    stitching_date: str
    apply_plugs: bool
    batch_size: int
    column_map: Dict[str, str]
    plug_constants: Dict[str, float]


class VnfMigrationProcessor:
    def __init__(self, config_path: str):
        self.config = self._load_config(config_path)
        self.stitching_date = pd.to_datetime(self.config.stitching_date)
        self.df = pd.DataFrame()

    def _load_config(self, path: str) -> VnfConfig:
        with open(path, "rb") as f:
            data = tomllib.load(f)

        return VnfConfig(
            input_file=data["base"]["input_file"],
            output_dir=data["base"]["output_dir"],
            stitching_date=data["base"]["stitching_date"],
            apply_plugs=data["logic"].get("apply_plugs", False),
            batch_size=data["output"].get("batch_size", 20),
            column_map=data["column_map"],
            plug_constants=data.get(
                "plugs",
                {
                    "default_cf_factor": 0.2,
                    "alt_mv_threshold": 0.1,
                    "numerator_sq_factor": 0.24,
                    "numerator_cross_factor": 0.2,
                    "denominator_val_factor": 1.2,
                    "denominator_mv_factor": 1.0,
                },
            ),
        )

    def load_data(self):
        """Loads and normalizes the input CSV based on column mapping."""
        df = pd.read_csv(self.config.input_file)

        # Rename columns based on config
        df = df.rename(columns=self.config.column_map)

        # Ensure required columns exist, fill with 0/appropriate defaults
        required_cols = [
            "Portfolio Firm Provided Key",
            "Date",
            "Value",
            "TWR_to_match",
            "FinTransfer",
            "OprTransfer",
            "Fees",
            "Expenses",
            "Household ID",
        ]

        for col in required_cols:
            if col not in df.columns:
                print(
                    f"Warning: Column '{col}' not found in input. Filling with 0/Empty."
                )
                df[col] = 0 if col != "Household ID" else "UNKNOWN"

        # Standardize Date
        df["Date"] = pd.to_datetime(df["Date"])

        # Filter based on stitching date
        # "Any values after this date should be ignored"
        df = df[df["Date"] <= self.stitching_date]

        # "Any values exactly equal to this date should be included, but rolled back one day"
        df.loc[df["Date"] == self.stitching_date, "Date"] = df.loc[
            df["Date"] == self.stitching_date, "Date"
        ] - pd.Timedelta(days=1)

        self.df = df.sort_values(by=["Portfolio Firm Provided Key", "Date"])

    def adjust_start_dates(self):
        """Moves the very first date of each account one day back."""
        # Find min date per account
        min_dates = self.df.groupby("Portfolio Firm Provided Key")[
            "Date"
        ].transform("min")

        # Shift those dates back by 1 day
        mask = self.df["Date"] == min_dates
        self.df.loc[mask, "Date"] = self.df.loc[mask, "Date"] - pd.Timedelta(
            days=1
        )

    def apply_algebraic_plugs(self):
        """
        Applies mathematical adjustments to Market Value and Cashflows
        to ensure TWR alignment.
        """
        if not self.config.apply_plugs:
            return

        df = self.df.copy()
        const = self.config.plug_constants

        # Calculate previous values
        df["MarketValuePrev"] = df.groupby("Portfolio Firm Provided Key")[
            "Value"
        ].shift(1)
        df["DatePrev"] = df.groupby("Portfolio Firm Provided Key")[
            "Date"
        ].shift(1)

        # Total CF for TWR check (Fin + Opr) - Notebook logic simplifies this generally to 'DateTransferredPosVal' equivalent
        # Here we assume FinTransfer + OprTransfer represents total flow impacting TWR for the plug calculation
        df["TotalCF"] = df["FinTransfer"] + df["OprTransfer"]

        # Calculate Manual Return: (End - Start - CF) / Start
        # Avoid div by zero
        df["PNL"] = df["Value"] - df["MarketValuePrev"] - df["TotalCF"]
        df["ManualReturn"] = np.where(
            df["MarketValuePrev"] != 0, df["PNL"] / df["MarketValuePrev"], 0
        )

        # Check Diff
        df["RetDiff"] = (df["ManualReturn"] - df["TWR_to_match"]).abs()

        # Identify rows needing plugs (Diff > 1bp)
        needs_plug = df["RetDiff"] > 0.0001

        # --- Plug Math ---
        # Constants from Config
        k_cf = const["default_cf_factor"]  # 0.2
        k_alt = const["alt_mv_threshold"]  # 0.1
        k_num_sq = const["numerator_sq_factor"]  # 0.24
        k_num_cross = const["numerator_cross_factor"]  # 0.2
        k_den_val = const["denominator_val_factor"]  # 1.2
        k_den_mv = const[
            "denominator_mv_factor"
        ]  # 1.0 (implied 1.0 * (1+TWR))

        # Initial CF Plug Guess
        df["CF_plug"] = df["Value"] * k_cf

        # Alternative Condition: EndVal <= 10% of StartVal
        is_alt = (df["Value"] <= df["MarketValuePrev"] * k_alt) & (
            df["Value"] > 0
        )
        df.loc[is_alt, "CF_plug"] = df.loc[is_alt, "MarketValuePrev"] * k_cf

        # Numerator
        # Default: (Val^2 * 0.24) - (Val * CF * 0.2)
        term1 = (df["Value"] ** 2) * k_num_sq
        term2 = df["Value"] * df["TotalCF"] * k_num_cross
        df["Numerator"] = term1 - term2

        # Alternative Numerator
        # (Prev^2 * 0.04) - (Prev * CF * 0.2) + (Prev * Val * 0.2)
        # Note: 0.04 seems derived from 0.2^2? Assuming raw mapping for now.
        # User requested constants in TOML, I'll use hardcoded for the derived ones or map them if strictly needed.
        # For simplicity adhering to "simple version", I will map main factors.
        # 0.04 is (0.2)^2.

        mask_alt = is_alt & needs_plug
        if mask_alt.any():
            prev = df.loc[mask_alt, "MarketValuePrev"]
            val = df.loc[mask_alt, "Value"]
            cf = df.loc[mask_alt, "TotalCF"]

            # Use 0.04 as k_cf^2? Or just hardcode as per notebook logic retention?
            # Keeping notebook logic structure:
            df.loc[mask_alt, "Numerator"] = (
                (prev**2 * 0.04)
                - (prev * cf * k_num_cross)
                + (prev * val * k_num_cross)
            )

        # Denominator
        # Default: Val*1.2 - Prev*(1+TWR) - CF
        term3 = df["Value"] * k_den_val
        term4 = df["MarketValuePrev"] * (1 + df["TWR_to_match"])
        df["Denominator"] = term3 - term4 - df["TotalCF"]

        # Alternative Denominator
        # Prev*0.2 - CF - Prev*(1+TWR) + Val
        if mask_alt.any():
            prev = df.loc[mask_alt, "MarketValuePrev"]
            val = df.loc[mask_alt, "Value"]
            cf = df.loc[mask_alt, "TotalCF"]
            twr = df.loc[mask_alt, "TWR_to_match"]

            df.loc[mask_alt, "Denominator"] = (
                (prev * k_cf) - cf - (prev * (1 + twr)) + val
            )

        # Calculate MV Plug
        df["MV_plug"] = df["Numerator"] / df["Denominator"]

        # Rounding
        df["CF_plug"] = df["CF_plug"].round(9)
        df["MV_plug"] = df["MV_plug"].round(9).fillna(0)

        # Determine Date for Plug (Day before)
        df["Date_plug"] = df["Date"] - pd.Timedelta(days=1)

        # Valid Plug Check: Must be different from Prev Date
        valid_plug = needs_plug & (df["Date_plug"] != df["DatePrev"])

        # --- Create Plug Rows ---
        plugs = df[valid_plug].copy()
        if not plugs.empty:
            # Map Plug columns to Main columns
            # The plug essentially creates a dummy entry with a Value and an offsetting Cashflow
            plugs_formatted = pd.DataFrame(
                {
                    "Portfolio Firm Provided Key": plugs[
                        "Portfolio Firm Provided Key"
                    ],
                    "Date": plugs["Date_plug"],
                    "Value": plugs["MV_plug"],
                    "FinTransfer": 0,  # Assuming plug flow goes to Opr or Fin? Notebook puts it in 'DateTransferredPosVal'
                    "OprTransfer": plugs["CF_plug"],  # Putting it here
                    "Fees": 0,
                    "Expenses": 0,
                    "Household ID": plugs["Household ID"],
                    "TWR_to_match": 0,  # No TWR on plug day
                    "IsPlug": True,
                }
            )

            # Concatenate
            self.df = pd.concat([self.df, plugs_formatted], ignore_index=True)
            self.df = self.df.sort_values(
                by=["Portfolio Firm Provided Key", "Date"]
            )

            # --- Adjust Original Rows ---
            # The row *after* the plug (the original row) needs its flow adjusted to offset the plug's flow
            # Notebook: original['DateTransferredPosVal'] -= plug['CF_plug']
            # We align by index or key. Since we just concat, let's re-find the rows.
            # Using merge for safety.

            # Key: Portfolio + Date (Original Date)
            adjustment_map = plugs[
                ["Portfolio Firm Provided Key", "Date", "CF_plug"]
            ].copy()
            # Rename Date back to original to match the target row
            adjustment_map = adjustment_map.rename(
                columns={"CF_plug": "Offset_CF"}
            )

            # We need to subtract the CF_plug from the OprTransfer of the original row
            # But wait, 'plugs' dataframe has 'Date_plug'. The original row has 'Date'.
            # We need to join on [Portfolio, Date] where adjustment_map['Date'] == original['Date']

            # Apply adjustment
            # Iterate or merge? Merge index is safer.
            self.df = self.df.merge(
                adjustment_map,
                on=["Portfolio Firm Provided Key", "Date"],
                how="left",
            )
            mask_adj = self.df["Offset_CF"].notna()
            # Subtract plug CF from OprTransfer (assuming Opr is the generic bucket)
            self.df.loc[mask_adj, "OprTransfer"] -= self.df.loc[
                mask_adj, "Offset_CF"
            ]
            self.df.drop(columns=["Offset_CF"], inplace=True)

    def triplicate_nodes(self, df_inputs: pd.DataFrame) -> pd.DataFrame:
        """
        Creates Base, MAIN, and COMPL nodes.
        Base: Full Data.
        MAIN: Structure node (0 values).
        COMPL: Compl node (Fees/Expenses only, 0 value).
        """
        base = df_inputs.copy()
        base["Currency Split Type"] = "0"

        main = df_inputs.copy()
        main["Currency Split Type"] = "MAIN"
        # Zero out metrics
        cols_to_zero = [
            "Value",
            "FinTransfer",
            "OprTransfer",
            "Fees",
            "Expenses",
        ]
        main[cols_to_zero] = 0

        compl = df_inputs.copy()
        compl["Currency Split Type"] = "COMPL"
        # Zero out metrics
        compl[cols_to_zero] = 0
        # Wait, notebook puts Fees/Expenses in COMPL?
        # Notebook: "now separate fees, expenses and taxes to COMPL node... remove those cash values from the other nodes"
        # So Base has 0 fees?
        # Notebook:
        # main = output_vnf.copy()... all 0
        # compl = output_vnf.copy()... all 0
        # output_vnf[['DateFees', ...]] = 0 (Base cleared of fees)
        # Wait, if compl is all 0, where do fees go?
        # Re-reading notebook carefully:
        # "now separate fees... main=copy... compl=copy... remove those cash values from the other nodes"
        # It seems I might have missed where COMPL gets the fees *assigned*.
        # Actually, standard VNF pattern:
        # Base: Value, Transfers.
        # COMPL: Fees, Expenses (Value=0).

        # Correct Logic:
        # Base: Keeps Value, Transfers. Sets Fees/Exp to 0.
        # Main: All 0.
        # COMPL: Keeps Fees, Expenses. Sets Value/Transfers to 0.

        # Restore Fees to COMPL
        compl["Fees"] = df_inputs["Fees"]
        compl["Expenses"] = df_inputs["Expenses"]

        # Clear Fees from Base
        base["Fees"] = 0
        base["Expenses"] = 0

        return pd.concat([base, main, compl], ignore_index=True)

    def generate_outputs(self):
        output_dir = Path(self.config.output_dir)
        os.makedirs(output_dir / "inputs", exist_ok=True)
        os.makedirs(output_dir / "portfolios", exist_ok=True)
        os.makedirs(output_dir / "bookvalues", exist_ok=True)

        # Group by Household
        households = self.df["Household ID"].unique()
        mapping_data = []

        # Chunking
        for i in range(0, len(households), self.config.batch_size):
            batch_index = i // self.config.batch_size
            batch_hh = households[i : i + self.config.batch_size]

            for hh in batch_hh:
                mapping_data.append(
                    {"Household ID": hh, "Batch Index": batch_index}
                )

            batch_df = self.df[self.df["Household ID"].isin(batch_hh)].copy()

            if batch_df.empty:
                continue

            # --- Inputs File ---
            # Apply triplication
            inputs_df = self.triplicate_nodes(batch_df)

            # Map to Final Output Columns
            # Required: Portfolio Firm Provided Key, Position Firm Provided Key, Date, Value,
            # DateTransferredPosVal, DateCashTransfer, DateFees, DateExtCashExpenses, Household ID, Quantity, NumUnits

            final_inputs = pd.DataFrame()
            final_inputs["Portfolio Firm Provided Key"] = inputs_df[
                "Portfolio Firm Provided Key"
            ]
            final_inputs["Position Firm Provided Key"] = (
                "history_instrument_USD"
            )
            final_inputs["Date"] = inputs_df["Date"]
            final_inputs["Currency Split Type"] = inputs_df[
                "Currency Split Type"
            ]
            final_inputs["Value"] = inputs_df["Value"]
            final_inputs["Quantity"] = inputs_df["Value"]
            final_inputs["NumUnits"] = inputs_df["Value"]

            # Transfers
            # FinTransfer -> DateCashTransfer (External)
            # OprTransfer -> DateTransferredPosVal (Internal/Generic)
            final_inputs["DateCashTransfer"] = inputs_df["FinTransfer"]
            final_inputs["DateTransferredPosVal"] = inputs_df["OprTransfer"]

            final_inputs["DateFees"] = inputs_df["Fees"]
            final_inputs["DateExtCashExpenses"] = inputs_df["Expenses"]
            final_inputs["DateExtCashTax"] = 0  # Default
            final_inputs["Household ID"] = inputs_df["Household ID"]

            # Save Inputs
            final_inputs.drop(columns="Household ID").to_csv(
                output_dir
                / "inputs"
                / f"own-analytics-set-{batch_index}.csv",
                index=False,
            )

            # --- Portfolios File ---
            # Distinct list of portfolios from Base data
            portfolios = pd.DataFrame()
            portfolios["Firm Provided Key"] = (
                batch_df["Portfolio Firm Provided Key"].astype(str)
                + "_PrimarySleeve"
            )
            portfolios = portfolios.drop_duplicates()

            # Save Portfolios
            portfolios.to_csv(
                output_dir
                / "portfolios"
                / f"portfolio-set-{batch_index}.csv",
                index=False,
            )

            # --- BookValues File (v5.2 USD) ---
            # Columns: PortfolioID, InstrumentID, Date, CurrencySplitType, DateTradeAmt,
            # DateFinTransfPosVal, DateOprTransfPosVal, ...

            bv = pd.DataFrame()
            bv["PortfolioID"] = batch_df["Portfolio Firm Provided Key"]
            bv["InstrumentID"] = "history_instrument_USD"
            bv["Date"] = batch_df["Date"]
            bv["CurrencySplitType"] = "0"
            bv["DateTradeAmt"] = (
                0  # Default for now as 'cash from trades' was not requested/implemented
            )

            # v5.2 Logic: Fin vs Opr Separation
            bv["DateFinTransfPosVal"] = batch_df["FinTransfer"]
            bv["DateOprTransfPosVal"] = batch_df["OprTransfer"]

            # Other 5.2 cols
            zero_cols = [
                "BookNumUnits",
                "BookValue",
                "DateTransferredCost",
                "DateRealizedPnl",
                "InternalBookNumUnits",
                "InternalBookValue",
                "DateInternalTransferredCost",
                "DateInternalRealizedPnl",
                "SettledBookValue",
                "DateFinTransfAccrVal",
                "DateOprTransfAccrVal",
            ]
            for c in zero_cols:
                bv[c] = 0

            # Save BV
            bv_dir = output_dir / "bookvalues" / f"bv-set-{batch_index}"
            os.makedirs(bv_dir, exist_ok=True)
            bv.to_csv(bv_dir / f"bv-set-{batch_index}_USD.csv", index=False)

        # --- Misc Files (Configs & Offsets) ---
        misc = MiscFiles(self.df, self.stitching_date)
        hist_conf, pres_conf = misc.create_portfolio_configurations_file()
        offset_trx = misc.create_offset_transactions()

        hist_conf.to_csv(
            output_dir / "PortfolioConfigs_Historical.csv", index=False
        )
        pres_conf.to_csv(
            output_dir / "PortfolioConfigs_Present.csv", index=False
        )
        offset_trx.to_csv(output_dir / "OffsetTransactions.csv", index=False)

        # Save Household Mapping
        pd.DataFrame(mapping_data).to_csv(
            output_dir / "HouseholdMapping.csv", index=False
        )

    def run(self):
        self.load_data()
        self.adjust_start_dates()
        self.apply_algebraic_plugs()
        self.generate_outputs()


class MiscFiles:
    def __init__(self, df: pd.DataFrame, stitching_date: pd.Timestamp):
        self.df = df
        self.stitching_date = stitching_date.strftime("%Y-%m-%d")

    def create_portfolio_configurations_file(
        self,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        # Historical Config
        historical = (
            self.df[["Portfolio Firm Provided Key", "Date"]]
            .copy()
            .drop_duplicates("Portfolio Firm Provided Key", keep="first")
            .reset_index(drop=True)
            .rename(
                columns={"Portfolio Firm Provided Key": "CustodianAccountID"}
            )
        )
        historical["SleeveID"] = [
            f"{acc}_PrimarySleeve"
            for acc in historical["CustodianAccountID"].astype(str)
        ]
        historical["Portfolio In Terms Of"] = "Transactions"
        historical["Tracking Type"] = "OwnAnalytics"
        historical["Are Splits Per Position"] = False
        historical["Are Cashflows Per Position"] = True

        # Present Config
        present = historical.copy()
        present["Date"] = self.stitching_date
        present["Tracking Type"] = "Transactions"

        return historical, present

    def create_offset_transactions(self) -> pd.DataFrame:
        # Filter for last date and non-zero value
        offset = self.df.copy()
        last_date = offset["Date"].max()

        # We need the Value at the stitching date (last date in DF) to offset it
        mask = (offset["Date"] == last_date) & (offset["Value"] != 0)
        offset = offset.loc[
            mask, ["Portfolio Firm Provided Key", "Value"]
        ].copy()

        offset = offset.rename(
            columns={
                "Portfolio Firm Provided Key": "Custodian Account ID",
                "Value": "Amount",
            }
        )

        # If Amount > 0 (Long), we need to Transfer Out to zero it.
        # If Amount < 0 (Short), we need to Transfer In to zero it.
        offset["Type"] = np.where(
            offset["Amount"] > 0,
            "Transfer Security Out",
            "Transfer Security In",
        )

        offset["Amount"] = offset["Amount"].abs()
        offset["Quantity"] = offset["Amount"]
        offset["Market Value in Transaction Currency"] = offset["Amount"]

        offset["Process Date"] = self.stitching_date
        offset["Settle Date"] = self.stitching_date
        offset["Trade Date"] = self.stitching_date
        offset["Currency Name"] = "USD"
        offset["Instrument ID"] = (
            "history_instrument_USD"  # Matching generic format
        )
        offset["Transaction ID"] = "vnf_transfer_" + offset[
            "Custodian Account ID"
        ].astype(str)

        return offset


def main():
    if len(sys.argv) < 2:
        print("Usage: python vnf_migration.py <config.toml>")
        return

    processor = VnfMigrationProcessor(sys.argv[1])
    processor.run()


if __name__ == "__main__":
    main()
