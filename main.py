import glob
import msvcrt
import os
import re
import warnings
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import pandas as pd
from rapidfuzz import fuzz, process

# Ignore unneeded warnings
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.simplefilter("ignore", category=FutureWarning)


@dataclass
class ColumnConfig:
    """
    Configuration for column names to make the code more maintainable.
    Only includes columns required for processing.
    """

    # Master list columns
    master_site_work: str = "Site Work"
    master_full_name: str = "Full Name"
    master_eid: str = "EID"

    # TCLL columns
    tcll_labor_distribution: str = "Labor Distribution"
    tcll_employee_number: str = "Emp #"
    tcll_first_name: str = "First Name"
    tcll_last_name: str = "Last Name"
    tcll_date: str = "Date"
    tcll_clock_in_time: str = "Clock In Time"
    tcll_clock_in_type: str = "Clock In Type"
    tcll_clock_out_time: str = "Clock Out Time"
    tcll_clock_out_type: str = "Clock Out Type"
    tcll_total_paid: str = "Total Paid"
    tcll_remove: str = "Remove"

    # PBJ columns
    pbj_employee_number: str = "EmployeeID"
    pbj_first_name: str = "FirstName"
    pbj_last_name: str = "LastName"
    pbj_position: str = "Position"
    pbj_date: str = "Date"
    pbj_hours: str = "Hours"
    pbj_pay_types_desc: str = "PayTypesDescription"
    pbj_labor_distribution: str = "Labor Distribution"

    # Rehab PBJ columns
    rehab_full_name: str = "Full Name"
    rehab_eid: str = "Employee No"
    rehab_site_work: str = "Site Worked"
    rehab_facility: str = "Primary Facility"


class PBJProcessor:
    """Main class for processing PBJ-related files."""

    def __init__(self, config: Optional[ColumnConfig] = None):
        self.config = config or ColumnConfig()

    def load_master(self, file_path: str, sheet_name: str = "Master") -> pd.DataFrame:
        """Load and validate the Contract Employee ID Master List."""
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Master file not found: {file_path}")

            df_master = pd.read_excel(
                file_path, sheet_name=sheet_name, dtype={"EID": str}
            )

            # Remove unnamed columns
            df_master = df_master.loc[:, ~df_master.columns.str.contains("^Unnamed")]

            # Validate required columns
            required_columns = {
                self.config.master_site_work,
                self.config.master_full_name,
                self.config.master_eid,
            }
            self._validate_columns(df_master, required_columns, "Master")

            # Filter to required columns only
            df_master = df_master[list(required_columns)]

            print(f"Loaded master list with {len(df_master)} records")
            return df_master

        except Exception as e:
            print(f"Error loading master file: {e}")
            raise

    def process_tcll(self, file_path: str) -> pd.DataFrame:
        """Load and process the Time Card by Labor Level sheet."""

        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"TCLL file not found: {file_path}")

            df_tcll = pd.read_excel(file_path)

            # Remove unnamed columns
            df_tcll = df_tcll.loc[:, ~df_tcll.columns.str.contains("^Unnamed")]

            # Validate required columns
            required_columns = {
                self.config.tcll_labor_distribution,
                self.config.tcll_employee_number,
                self.config.tcll_clock_in_type,
                self.config.tcll_clock_out_type,
                self.config.tcll_total_paid,
            }
            self._validate_columns(df_tcll, required_columns, "TCLL")

            # Parse dates if Date column exists
            df_tcll = self._parse_dates(df_tcll)

            # Apply filters and transformations
            df_tcll = self._apply_tcll_filters(df_tcll)

            print(f"Processed TCLL with {len(df_tcll)} records")
            return df_tcll

        except Exception as e:
            print(f"Error processing TCLL file: {e}")
            raise

    def _apply_tcll_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all TCLL-specific filters and transformations."""
        # Make a copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Filter for Total Paid > 8.00
        hour_threshold = 8.00
        df = df[df[self.config.tcll_total_paid] > hour_threshold].copy()

        # Filter out Clock In Time == 8:00 AND Clock Out Time == 16:30
        df = df[
            ~(
                (df[self.config.tcll_clock_in_time] == "8:00")
                & (df[self.config.tcll_clock_out_time] == "16:30")
            )
        ].copy()

        # Clean Labor Distribution
        df.loc[:, self.config.tcll_labor_distribution] = df[
            self.config.tcll_labor_distribution
        ].apply(self._remove_duplicates)

        # Convert Employee Number to integer
        df.loc[:, self.config.tcll_employee_number] = pd.to_numeric(
            df[self.config.tcll_employee_number], errors="coerce"
        ).astype("Int64")

        # Filter clock types
        clock_in_filter = ["Clock In", "Work Day Split"]
        clock_out_filter = ["Clock Out", "Work Day Split"]

        df = df[
            df[self.config.tcll_clock_in_type].isin(clock_in_filter)
            & df[self.config.tcll_clock_out_type].isin(clock_out_filter)
        ].copy()

        # Calculate Remove column (deductions)
        deduction_interval = 8
        deduction_per_interval = -0.5
        df.loc[:, self.config.tcll_remove] = deduction_per_interval * (
            df[self.config.tcll_total_paid] // deduction_interval
        )

        return df

    def process_pbj(self, file_path: str) -> pd.DataFrame:
        """Load and process the regular PBJ CSV."""
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"PBJ file not found: {file_path}")

            df_pbj = pd.read_csv(file_path, dtype={"Level 1": str, "Level 2": str})

            # Remove unnamed columns
            df_pbj = df_pbj.loc[:, ~df_pbj.columns.str.contains("^Unnamed")]

            # Validate required columns
            required_columns = {
                self.config.pbj_pay_types_desc,
                self.config.pbj_labor_distribution,
            }
            self._validate_columns(df_pbj, required_columns, "PBJ")

            # Parse dates if Date column exists
            df_pbj = self._parse_dates(df_pbj)

            # Apply filters and transformations
            df_pbj = self._apply_pbj_filters(df_pbj)

            print(f"Processed PBJ with {len(df_pbj)} records")
            return df_pbj

        except Exception as e:
            print(f"Error processing PBJ file: {e}")
            raise

    def _apply_pbj_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all PBJ-specific filters and transformations."""
        # Make a copy to avoid SettingWithCopyWarning
        df = df.copy()

        # Filter for 'Work' pay types
        df = df[df[self.config.pbj_pay_types_desc] == "Work"].copy()

        # Exclude Rehab job descriptions
        rehab_exclusions = [
            "Physician Assistant",
            "Occupational Therapist",
            "Occupational Therapy Assistant",
            "Occupational Therapy Aide",
            "Physical Therapist",
            "Physical Therapy Assistant",
            "Physical Therapy Aide",
            "Speech/Language Pathologist",
        ]
        df = df[~df[self.config.pbj_labor_distribution].isin(rehab_exclusions)].copy()

        # Rename specific labor distributions
        df.loc[:, self.config.pbj_labor_distribution] = df[
            self.config.pbj_labor_distribution
        ].apply(self._rename_labor_distribution)

        return df

    def merge_tcll_pbj(
        self, df_tcll: pd.DataFrame, df_pbj: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge TCLL and PBJ DataFrames."""
        try:
            tcll_mapping = {
                "eid": self.config.tcll_employee_number,
                "first_name": self.config.tcll_first_name,
                "last_name": self.config.tcll_last_name,
                "date": self.config.tcll_date,
                "hours": self.config.tcll_remove,
                "labor_distribution": self.config.tcll_labor_distribution,
            }
            pbj_mapping = {
                "eid": self.config.pbj_employee_number,
                "first_name": self.config.pbj_first_name,
                "last_name": self.config.pbj_last_name,
                "date": self.config.pbj_date,
                "hours": self.config.pbj_hours,
                "labor_distribution": self.config.pbj_labor_distribution,
            }

            # Find which TCLL columns exist and can be mapped
            tcll_columns_to_use = []
            rename_mapping = {}

            for standard_name, tcll_col in tcll_mapping.items():
                if tcll_col in df_tcll.columns and standard_name in pbj_mapping:
                    pbj_col = pbj_mapping[standard_name]
                    tcll_columns_to_use.append(tcll_col)
                    rename_mapping[tcll_col] = pbj_col

            if not tcll_columns_to_use:
                print("Error: No matching columns found for TCLL merge")
                return df_pbj

            # Select only the defined columns from TCLL and rename them
            df_tcll_processed = df_tcll[tcll_columns_to_use].copy()
            df_tcll_processed = df_tcll_processed.rename(columns=rename_mapping)

            # Ensure TCLL has all PBJ columns (fill missing with pd.NA)
            df_tcll_aligned = df_tcll_processed.reindex(
                columns=df_pbj.columns, fill_value=pd.NA
            )

            # Handle empty DataFrames (concat only when non-empty)
            if df_pbj.empty and df_tcll_aligned.empty:
                merged_df = pd.DataFrame(columns=df_pbj.columns)
            elif df_pbj.empty:
                merged_df = df_tcll_aligned.copy()
            elif df_tcll_aligned.empty:
                merged_df = df_pbj.copy()
            else:
                merged_df = pd.concat(
                    [df_pbj, df_tcll_aligned], ignore_index=True, sort=False
                )

            print(
                f"Merged data: {len(df_pbj)} PBJ + {len(df_tcll_processed)} TCLL = {len(merged_df)} total records"
            )
            return merged_df

        except Exception as e:
            print(f"Error merging TCLL and PBJ: {e}")
            raise

    def process_rehab_pbj(
        self,
        file_path: str,
        df_masterlist: pd.DataFrame,
        name_match_threshold: int = 90,
    ) -> List[pd.DataFrame]:
        """Process rehab PBJ using masterlist for EID lookup."""
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"Rehab PBJ file not found: {file_path}")

            df_rehab_pbj = pd.read_excel(file_path)
            df_rehab_pbj = df_rehab_pbj.loc[
                :, ~df_rehab_pbj.columns.str.contains("^Unnamed")
            ]

            # Validate columns
            required_columns = {
                self.config.rehab_full_name,
                self.config.rehab_eid,
                self.config.rehab_site_work,
                self.config.rehab_facility,
            }
            self._validate_columns(df_rehab_pbj, required_columns, "Rehab PBJ")

            # Parse dates if Date column exists
            df_rehab_pbj = self._parse_dates(df_rehab_pbj)

            # Group by site and process each group
            processed_groups = []
            for group_name, group_df in df_rehab_pbj.groupby(
                self.config.rehab_site_work
            ):
                print(f"\nProcessing group: {group_name}")
                processed_group = self._match_rehab_eids(
                    group_df, df_masterlist, name_match_threshold
                ).reset_index(drop=True)

                processed_groups.append(processed_group)
                print(f"Completed processing for group: {group_name}")

            print(f"\nProcessed Rehab PBJ into {len(processed_groups)} site groups")
            return processed_groups

        except Exception as e:
            print(f"Error processing Rehab PBJ: {e}")
            raise

    def _match_rehab_eids(
        self, df_rehab: pd.DataFrame, df_master: pd.DataFrame, threshold: int
    ) -> pd.DataFrame:
        """Match rehab employee names to master list EIDs."""
        # Create working copies with normalized columns
        df_rehab = df_rehab.copy()
        df_master = df_master.copy()

        # Normalize names and sites
        df_rehab.loc[:, "_norm_name"] = df_rehab[self.config.rehab_full_name].apply(
            self._normalize_name
        )
        df_rehab.loc[:, "_norm_site_work"] = df_rehab[
            self.config.rehab_site_work
        ].apply(self._normalize_site)
        df_rehab.loc[:, "_norm_facility"] = df_rehab[self.config.rehab_facility].apply(
            self._normalize_site
        )

        df_master.loc[:, "_norm_name"] = df_master[self.config.master_full_name].apply(
            self._normalize_name
        )
        df_master.loc[:, "_norm_site_work"] = df_master[
            self.config.master_site_work
        ].apply(self._normalize_site)

        # Track processed entries to avoid duplicate logging
        seen_log_keys = set()
        matches_found = 0

        for idx, row in df_rehab.iterrows():
            if (
                row["_norm_site_work"] != row["_norm_facility"]
                and row["_norm_facility"] != ""
            ):
                log_key = (row["_norm_name"], row["_norm_site_work"])

                # Find site matches
                site_matches = df_master[
                    df_master["_norm_site_work"].apply(
                        lambda s: row["_norm_site_work"] in s
                        or s in row["_norm_site_work"]
                    )
                ]

                if not site_matches.empty:
                    # Find name match
                    result = process.extract(
                        row["_norm_name"],
                        site_matches["_norm_name"].tolist(),
                        scorer=fuzz.token_sort_ratio,
                    )
                    name_matches = [match for match in result if match[1] >= threshold]

                    # Multiple name matches :: clear EID
                    if len(name_matches) > 1:
                        df_rehab.loc[idx, self.config.rehab_eid] = ""
                        if log_key not in seen_log_keys:
                            print(
                                f"[MULTIPLE MATCHES]           {row[self.config.rehab_full_name]}"
                            )
                            seen_log_keys.add(log_key)

                    # Single name match found :: swap EID
                    elif len(name_matches) == 1:
                        # Get the matched name from the tuple (name, score, index)
                        matched_name = name_matches[0][0]
                        matched_row = site_matches[
                            site_matches["_norm_name"] == matched_name
                        ].iloc[0]
                        df_rehab.loc[idx, self.config.rehab_eid] = str(
                            matched_row[self.config.master_eid]
                        )
                        matches_found += 1

                        if log_key not in seen_log_keys:
                            print(
                                f"[SINGLE MATCH]     {matched_row[self.config.master_eid]}"
                                f"   {row[self.config.rehab_full_name]}"
                            )
                            seen_log_keys.add(log_key)

                    # Name match not found :: clear EID
                    else:
                        df_rehab.loc[idx, self.config.rehab_eid] = ""
                        if log_key not in seen_log_keys:
                            print(
                                f"[NO MATCH]                   {row[self.config.rehab_full_name]}"
                            )
                            seen_log_keys.add(log_key)

                # Site match not found :: clear EID
                else:
                    df_rehab.loc[idx, self.config.rehab_eid] = ""
                    if log_key not in seen_log_keys:
                        print(f"[NO MATCH]  {row[self.config.rehab_site_work]}")
                        seen_log_keys.add(log_key)

        # Clean up temporary columns
        temp_cols = ["_norm_name", "_norm_site_work", "_norm_facility"]
        df_rehab.drop(columns=temp_cols, inplace=True, errors="ignore")
        df_master.drop(
            columns=["_norm_name", "_norm_site_work"], inplace=True, errors="ignore"
        )

        print(f"Found {matches_found} EID matches out of {len(df_rehab)} records")
        return df_rehab

    def process_pbj_files(
        self,
        input_dir: str = "input",
        processed_dir: str = "output/processed",
        merged_dir: str = "output/merged",
    ) -> None:
        """Process all PBJ and TCLL files in the input directory."""
        try:
            # Create output directories
            Path(processed_dir).mkdir(parents=True, exist_ok=True)
            Path(merged_dir).mkdir(parents=True, exist_ok=True)

            # Find PBJ files
            pbj_files = glob.glob(f"{input_dir}/*Payroll Based Journal.csv")

            if not pbj_files:
                print("Error: No PBJ files found")
                return

            print(f"Found {len(pbj_files)} PBJ files to process")

            successful = 0
            for pbj_file in pbj_files:
                try:
                    success = self._process_single_pbj_file(
                        pbj_file, input_dir, processed_dir, merged_dir
                    )
                    if success:
                        successful += 1
                except Exception as e:
                    print(f"Error processing {Path(pbj_file).name}: {e}")
                    continue

            print(
                f"\nSuccessfully processed {successful}/{len(pbj_files)} PBJ files\n"
            )

        except Exception as e:
            print(f"Error in process_pbj_files: {e}")
            raise

    def _process_single_pbj_file(
        self, pbj_file: str, input_dir: str, processed_dir: str, merged_dir: str
    ) -> bool:
        """Process a single PBJ file and its corresponding TCLL file."""
        pbj_filename = Path(pbj_file).name

        # Parse filename to extract quarter and facility
        match = re.match(
            r"(\d{4} Q\d+)\s+(.+?)\s+Payroll Based Journal\.csv", pbj_filename
        )
        if not match:
            print(f"Error: Could not parse filename format: {pbj_filename}")
            return False

        quarter, facility_name = match.groups()
        print(f"\nProcessing {quarter} {facility_name}...")

        # Find corresponding TCLL file
        tcll_pattern = (
            f"{input_dir}/*{quarter}*{facility_name}*Time Card by Labor Level.xlsx"
        )
        tcll_files = glob.glob(tcll_pattern)

        if not tcll_files:
            print(f"Error: No TCLL file found for {quarter} {facility_name}")
            return False

        tcll_file = tcll_files[0]
        tcll_filename = Path(tcll_file).name

        # Process files
        processed_pbj = self.process_pbj(pbj_file)
        processed_tcll = self.process_tcll(tcll_file)
        merged_data = self.merge_tcll_pbj(processed_tcll, processed_pbj)

        # Export files
        processed_pbj_path = Path(processed_dir) / pbj_filename.replace(".csv", ".xlsx")
        processed_tcll_path = Path(processed_dir) / tcll_filename
        merged_path = Path(merged_dir) / f"{quarter} {facility_name} PBJ.xlsx"

        processed_pbj.to_excel(processed_pbj_path, index=False)
        processed_tcll.to_excel(processed_tcll_path, index=False)
        merged_data.to_excel(merged_path, index=False)

        print(f"{quarter} {facility_name} processed successfully")
        print(f"  - Processed PBJ: {processed_pbj_path}")
        print(f"  - Processed TCLL: {processed_tcll_path}")
        print(f"  - Merged output: {merged_path}")

        return True

    def process_rehab_pbj_files(
        self, input_dir: str = "input", output_dir: str = "output"
    ) -> None:
        """Process all Rehab PBJ files."""
        try:
            Path(output_dir).mkdir(parents=True, exist_ok=True)

            # Load master list
            masterlist_files = glob.glob(f"{input_dir}/*Master List*.xlsx")
            if not masterlist_files:
                raise FileNotFoundError("Master list file not found")

            df_master = self.load_master(masterlist_files[0])
            master_output = Path(output_dir) / "Master List.xlsx"
            df_master.to_excel(master_output, index=False)

            # Find Rehab PBJ files
            rehab_files = glob.glob(f"{input_dir}/*Rehab PBJ.xlsx")

            if not rehab_files:
                print("Error: No Rehab PBJ files found")
                return

            print(f"Found {len(rehab_files)} Rehab PBJ files to process")

            successful = 0
            for rehab_file in rehab_files:
                try:
                    success = self._process_single_rehab_file(
                        rehab_file, df_master, output_dir
                    )
                    if success:
                        successful += 1
                except Exception as e:
                    print(f"Error processing {Path(rehab_file).name}: {e}")
                    continue

            print(
                f"\nSuccessfully processed {successful}/{len(rehab_files)} Rehab PBJ files"
            )

        except Exception as e:
            print(f"Error in process_rehab_pbj_files: {e}")
            raise

    def _process_single_rehab_file(
        self, rehab_file: str, df_master: pd.DataFrame, output_dir: str
    ) -> bool:
        """Process a single Rehab PBJ file."""
        rehab_filename = Path(rehab_file).name

        # Parse filename
        match = re.match(r"(\d{4} Q\d+)\s+(.+?)\s+Rehab PBJ\.xlsx", rehab_filename)
        if not match:
            print(f"Error: Could not parse rehab filename format: {rehab_filename}")
            return False

        quarter, facility_name = match.groups()
        print(f"\nProcessing {quarter} Rehab PBJ...")

        # Process the file
        df_rehab_groups = self.process_rehab_pbj(rehab_file, df_master)

        # Export each site's data
        for df_site in df_rehab_groups:
            if df_site.empty:
                continue

            site_name = df_site[self.config.rehab_site_work].iloc[0]
            output_filename = f"{quarter} {site_name} Rehab PBJ.xlsx"
            output_path = Path(output_dir) / output_filename
            df_site.to_excel(output_path, index=False)
            print(f"  - Rehab output: {output_path}")

        return True

    # Utility methods
    @staticmethod
    def _validate_columns(
        df: pd.DataFrame, required_columns: set, file_type: str
    ) -> None:
        """Validate that required columns exist in the DataFrame."""
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(
                f"Missing required {file_type} columns: {missing_columns}. "
                "Perhaps the columns were renamed?"
            )

    @staticmethod
    def _parse_dates(df: pd.DataFrame) -> pd.DataFrame:
        """Parse date columns and remove time components."""
        df = df.copy()
        date_columns = ["Date", "date", "DATE", "Work Date", "Pay Date", "Labor Date"]
        for col in df.columns:
            if col in date_columns or "date" in col.lower():
                try:
                    # Check if column contains date-like data
                    if df[
                        col
                    ].dtype == "object" or pd.api.types.is_datetime64_any_dtype(
                        df[col]
                    ):
                        # Convert to datetime and extract date only
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
                        # print(f"Parsed date column: {col}")
                except Exception as e:
                    print(f"Error: Could not parse date column {col}: {e}")
                    continue
        return df

    @staticmethod
    def _remove_duplicates(text, sep: str = " - ") -> str:
        """Remove duplicate text separated by a separator."""
        if pd.isna(text):
            return text
        text_str = str(text)
        parts = text_str.split(sep)
        return parts[0].strip() if len(parts) > 1 else text_str.strip()

    @staticmethod
    def _rename_labor_distribution(text: str) -> str:
        """Rename specific labor distribution values."""
        replacements = {
            "MDS - RN": "RN with Admin Duties",
            "MDS - LVN": "LVN with Admin Duties",
        }
        return replacements.get(text, text)

    @staticmethod
    def _normalize_name(name: str) -> str:
        """Normalize names for fuzzy matching."""
        if pd.isna(name):
            return ""

        name = str(name)
        # Remove quoted text
        name = re.sub(r'"[^"]*"', "", name)
        name = name.replace(".", "").strip().lower()

        # Handle "Last, First" format
        if "," in name:
            parts = [part.strip() for part in name.split(",")]
            name = f"{parts[1]} {parts[0]}" if len(parts) > 1 else parts[0]

        # Split into tokens and filter
        tokens = name.split()
        tokens = [t for t in tokens if len(t) > 1]  # Remove single characters

        # Remove common suffixes
        suffixes = {"jr", "sr", "ii", "iii", "iv", "v"}
        tokens = [t for t in tokens if t not in suffixes]

        return " ".join(tokens)

    @staticmethod
    def _normalize_site(site: str) -> str:
        """Normalize site names for consistent matching across datasets."""
        if pd.isna(site):
            return ""

        site = str(site).lower()
        site = site.replace("&", " and ")
        site = site.replace("-", " ")
        site = site.replace(".", "").replace("'", "")
        site = re.sub(r"\s+", " ", site).strip()

        # Normalize 'health care' and 'health-care' to 'healthcare'
        site = re.sub(r"\bhealth[\s\-]?care\b", "healthcare", site)

        # Remove interchangeable, generic terms
        interchangeable_keywords = [
            "post acute",
            "acute",
            "care",
            "center",
            "facility",
            "rehab",
            "convalescent hospital",
            "new",
        ]

        for keyword in interchangeable_keywords:
            pattern = r"\b" + re.escape(keyword) + r"\b"
            site = re.sub(pattern, "", site)

        # Collapse extra spaces again after removals
        site = re.sub(r"\s+", " ", site).strip()

        return site


def main():
    """Main function to run the PBJ processing."""
    try:
        processor = PBJProcessor()

        # For regular PBJ processing:
        processor.process_pbj_files(
            input_dir="input",
            processed_dir="output/processed",
            merged_dir="output/merged",
        )

        # For Rehab PBJ processing:
        processor.process_rehab_pbj_files(input_dir="input", output_dir="output/rehab")

        print("\nAll processing completed successfully!")

        if os.name == "nt":
            print("Press any key to exit...")
            msvcrt.getch()
        else:
            input("Press Enter to exit...")

    except Exception as e:
        print(f"\nProcessing failed: {e}")
        raise


if __name__ == "__main__":
    main()
