import glob
import msvcrt
import os
import re
import warnings
from dataclasses import dataclass
from collections import defaultdict
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


@dataclass
class PathGroup:
    pbj_path: Path
    tcll_path: Path

class PBJProcessor:
    """Main class for processing PBJ-related files."""

    def __init__(
        self,
        input_dir: str,
        processed_dir: str,
        merged_dir: str,
        rehab_dir: str,
        config: Optional[ColumnConfig] = None,
    ):
        self.input_dir: str = input_dir
        self.processed_dir: str = processed_dir
        self.merged_dir: str = merged_dir
        self.rehab_dir: str = rehab_dir
        self.config = config or ColumnConfig()
        
        self.df_master: pd.Dataframe = self.load_master()
        self.group_dict: defaultdict = defaultdict(dict)
        self.load_group_dict()
        self.rehab_dict: defaultdict = defaultdict(list)
        self.load_rehab_dict()

    def load_master(self, sheet_name: str = "Master") -> pd.DataFrame:
        """Load and validate the Contract Employee ID Master List."""
        try:
            masterlist_files = glob.glob(f"{self.input_dir}/*Master List*.xlsx")
            if not masterlist_files:
                raise FileNotFoundError("Master list file not found")
            file_path = masterlist_files[0] 
            df_master = pd.read_excel(file_path, sheet_name=sheet_name, dtype={"EID": str})

            # Validate columns and filter for required columns only
            df_master = df_master.loc[:, ~df_master.columns.str.contains("^Unnamed")]
            required_columns = {
                self.config.master_site_work,
                self.config.master_full_name,
                self.config.master_eid,
            }
            self._validate_columns(df_master, required_columns, "Master")
            df_master = df_master[list(required_columns)]

            print(f"Loaded master list with {len(df_master)} records")
            return df_master

        except Exception as e:
            print(f"Error loading master file: {e}")
            raise

    def load_group_dict(self) -> None:
        """Read input directory and group PBJ files by Quarter and Facility."""
        try:
            pbj_files = glob.glob(f"{self.input_dir}/*Payroll Based Journal.csv")
            if not pbj_files:
                raise FileNotFoundError("No PBJ files found")
            print(f"Found {len(pbj_files)} PBJ files to process")

            for pbj_file in pbj_files:
                # Extract quarter and facility from PBJ
                pbj_filename = Path(pbj_file).name
                match = re.match(
                    r"(\d{4} Q\d+)\s+(.+?)\s+Payroll Based Journal\.csv", pbj_filename
                )
                if not match:
                    print(f"Error: Could not parse filename format: {pbj_filename}")
                    continue
                quarter, facility = match.groups()
                self.group_dict[quarter][facility] = PathGroup(
                    pbj_path=Path(self.input_dir) / pbj_filename,
                    tcll_path=Path(),
                )

                # Find corresponding TCLL for quarter and facility (if any)
                tcll_pattern = (
                    f"{self.input_dir}/*{quarter}*{facility}*Time Card by Labor Level.xlsx"
                )
                tcll_files = glob.glob(tcll_pattern)
                if tcll_files:
                    tcll_file = tcll_files[0]
                    tcll_filename = Path(tcll_file).name
                    self.group_dict[quarter][facility] = PathGroup(
                        pbj_path=Path(self.input_dir) / pbj_filename,
                        tcll_path=Path(self.input_dir) / tcll_filename,
                    )
                
                print(f"Found files for {quarter} {facility}:")
                print(f"  - PBJ:  {self.group_dict[quarter][facility].pbj_path}")
                print(f"  - TCLL: {self.group_dict[quarter][facility].tcll_path}")
        
        except Exception as e:
            print(f"Error loading PBJ files: {e}")
            raise
    
    def load_rehab_dict(self) -> None:
        """Read input directory and group rehab PBJ files by Quarter."""
        try:
            rehab_files = glob.glob(f"{self.input_dir}/*Rehab PBJ.xlsx")
            if not rehab_files:
                raise FileNotFoundError("No Rehab PBJ files found")
            print(f"Found {len(rehab_files)} Rehab PBJ files to process")

            for rehab_file in rehab_files:
                # Extract quarter from Rehab PBJ
                rehab_filename = Path(rehab_file).name
                match = re.match(
                    r"(\d{4} Q\d+)\s+(.+?)\s+Rehab PBJ\.xlsx", rehab_filename
                )
                if not match:
                    print(f"Error: Could not parse filename format: {rehab_filename}")
                    continue
                quarter, facility = match.groups()
                self.rehab_dict[quarter].append(rehab_file)
            
            for quarter in self.rehab_dict.keys():
                print(f"Found files for {quarter}:")
                for rehab_file in self.rehab_dict[quarter]:
                    print(f"  - {rehab_file}")
        
        except Exception as e:
            print(f"Error loading Rehab PBJ files: {e}")
            raise

    def process_tcll(self, file_path: str) -> pd.DataFrame:
        """Load and process the Time Card by Labor Level sheet."""
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"TCLL file not found: {file_path}")

            df_tcll = pd.read_excel(file_path)

            # Validate and clean columns
            df_tcll = df_tcll.loc[:, ~df_tcll.columns.str.contains("^Unnamed")]
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

    def process_pbj(self, file_path: str, filter: bool = True) -> pd.DataFrame:
        """Load and process the regular PBJ CSV."""
        try:
            if not Path(file_path).exists():
                raise FileNotFoundError(f"PBJ file not found: {file_path}")

            df_pbj = pd.read_csv(file_path, dtype={"Level 1": str, "Level 2": str})

            # Validate and clean columns
            df_pbj = df_pbj.loc[:, ~df_pbj.columns.str.contains("^Unnamed")]
            required_columns = {
                self.config.pbj_pay_types_desc,
                self.config.pbj_labor_distribution,
            }
            self._validate_columns(df_pbj, required_columns, "PBJ")

            # Parse dates if Date column exists
            df_pbj = self._parse_dates(df_pbj)

            # Apply filters and transformations
            if filter:
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

    def merge_tcll_pbj(self, df_tcll: pd.DataFrame, df_pbj: pd.DataFrame) -> pd.DataFrame:
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

    def process_rehab_pbj(self, file_path: str, quarter: str, name_match_threshold: int = 90) -> List[pd.DataFrame]:
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
                processed_group = self._match_rehab_eids(group_df, group_name, quarter, name_match_threshold).reset_index(drop=True)
                processed_groups.append(processed_group)
                print("Completed processing for group.")

            print(f"\nProcessed Rehab PBJ into {len(processed_groups)} site groups")
            return processed_groups

        except Exception as e:
            print(f"Error processing Rehab PBJ: {e}")
            raise

    def _match_rehab_eids(self, df_rehab: pd.DataFrame, group_name: str, quarter: str, threshold: int) -> pd.DataFrame:
        """Match rehab employee names to master list EIDs."""
        # Create working copies with normalized columns
        df_rehab = df_rehab.copy()
        df_master = self.df_master.copy()

        # Normalize names and sites
        df_rehab.loc[:, "_norm_name"] = df_rehab[self.config.rehab_full_name].apply(self._normalize_name)
        df_rehab.loc[:, "_norm_site_work"] = df_rehab[self.config.rehab_site_work].apply(self._normalize_site)
        df_rehab.loc[:, "_norm_facility"] = df_rehab[self.config.rehab_facility].apply(self._normalize_site)
        
        df_master.loc[:, "_norm_name"] = df_master[self.config.master_full_name].apply(self._normalize_name)
        df_master.loc[:, "_norm_site_work"] = df_master[self.config.master_site_work].apply(self._normalize_site)

        # Cache already searched-keys for faster lookups
        eid_cache: dict[tuple(str, str, str), str] = {}
        matches_found = 0

        # Try to load the corresponding standard PBJ from the same group
        current_facility = group_name
        df_group_pbj: pd.DataFrame | None = None
        try:
            for group_facility in self.group_dict[quarter].keys():
                if self._match_site_alias(group_facility, current_facility):
                    group_pbj_path = self.group_dict[quarter][group_facility].pbj_path
                    df_group_pbj = self.process_pbj(group_pbj_path, filter=False)
                    break
        except Exception as e:
            print(f"Error loading PBJ for {group_name}: {e}")
            df_group_pbj = None

        # No corresponding PBJ :: keep unprocessed EID without checking.
        if df_group_pbj is None:
            print(f"Warning: Standard PBJ File for {group_name} not found. Standard EID validation disabled.")
                
        # Iterate through rows to validate/replace EIDs
        for idx, row in df_rehab.iterrows():
            cache_key = (row["_norm_name"], row["_norm_site_work"], row["_norm_facility"])
            cache_eid = ""
            
            # Check cache for valid EID
            if cache_key in eid_cache.keys():
                cache_eid = eid_cache[cache_key]
                matches_found += 1

            # Check PBJ for standard EID
            elif (row["_norm_site_work"] == row["_norm_facility"] and
                row["_norm_facility"] != ""):
                if df_group_pbj is not None and not df_group_pbj.empty:
                    # Create normalized names for PBJ employees
                    df_group_pbj_temp = df_group_pbj.copy()
                    df_group_pbj_temp['_norm_full_name'] = (
                        df_group_pbj_temp[self.config.pbj_first_name].astype(str) + " " + 
                        df_group_pbj_temp[self.config.pbj_last_name].astype(str)
                    ).apply(self._normalize_name)
                    
                    # Find name match in PBJ
                    result = process.extract(
                        row["_norm_name"],
                        df_group_pbj_temp["_norm_full_name"].tolist(),
                        scorer=fuzz.token_sort_ratio,
                    )
                    name_matches = [match for match in result if match[1] >= threshold]

                    # Name match found :: get EID from PBJ
                    if len(name_matches) >= 1:
                        matched_eids = set()
                        for match in name_matches:
                            matched_name = match[0]
                            matched_row = df_group_pbj_temp[df_group_pbj_temp["_norm_full_name"] == matched_name].iloc[0]
                            matched_eid = matched_row[self.config.pbj_employee_number]
                            matched_eids.add(matched_eid)
                        
                        # Replace EID with match if unique, else clear
                        if len(matched_eids) == 1:
                            cache_eid = list(matched_eids)[0]
                            matches_found += 1
                            print(
                                f"[Standard EID]  {cache_eid}     "
                                f"{row[self.config.rehab_full_name]}"
                            )
                        else:
                            print(
                                "[Standard EID]  Conflict  "
                                f"{row[self.config.rehab_full_name]}"
                            )
                    
                    # Name match not found :: clear EID
                    else:
                        print(
                            "[Standard EID]  No Match  "
                            f"{row[self.config.rehab_full_name]}"
                        )

                # Save EID to cache
                eid_cache[cache_key] = cache_eid

            # Check Master List for Contract EID
            elif (row["_norm_site_work"] != row["_norm_facility"] and 
                row["_norm_facility"] != ""):
                
                # Find site matches
                site_matches = df_master[df_master["_norm_site_work"].apply(
                        lambda s: row["_norm_site_work"] in s
                        or s in row["_norm_site_work"])]
                
                # Find name match
                if not site_matches.empty:
                    result = process.extract(
                        row["_norm_name"],
                        site_matches["_norm_name"].tolist(),
                        scorer=fuzz.token_sort_ratio,
                    )
                    name_matches = [match for match in result if match[1] >= threshold]
                    
                    # Single name match found :: swap EID
                    if len(name_matches) >= 1:
                        matched_eids = set()
                        for match in name_matches:
                            matched_name = match[0]
                            matched_row = site_matches[site_matches["_norm_name"] == matched_name].iloc[0]
                            matched_eid = str(matched_row[self.config.master_eid])
                            matched_eids.add(matched_eid)

                        if len(matched_eids) == 1:
                            cache_eid = list(matched_eids)[0]
                            matches_found += 1
                            print(
                                f"[Contract EID]  {cache_eid}     "
                                f"{row[self.config.rehab_full_name]}"
                            )
                        else:
                            print(
                                "[Contract EID]  Conflict  "
                                f"{row[self.config.rehab_full_name]}"
                            )

                    # Name match not found :: clear EID
                    else:
                        print(
                            "[Contract EID]  No Match  "
                            f"{row[self.config.rehab_full_name]}"
                        )
                
                # Site match not found :: clear EID
                else:
                    print(
                        "[Contract EID]  No Match  "
                        f"{row[self.config.rehab_site_work]}"
                    )
                
                # Save EID to cache
                eid_cache[cache_key] = cache_eid
        
            # Apply retrieved EID from Cache/PBJ/Master
            df_rehab.loc[idx, self.config.rehab_eid] = cache_eid

        # Clean up temporary columns
        temp_cols = ["_norm_name", "_norm_site_work", "_norm_facility"]
        df_rehab.drop(columns=temp_cols, inplace=True)
        df_master.drop(columns=["_norm_name", "_norm_site_work"], inplace=True)

        print(f"Found {matches_found} EID matches out of {len(df_rehab)} records")
        return df_rehab

    def process_pbj_files(self) -> None:
        """Process all PBJ and TCLL files in the input directory."""
        try:
            # Create output directories
            Path(self.processed_dir).mkdir(parents=True, exist_ok=True)
            Path(self.merged_dir).mkdir(parents=True, exist_ok=True)

            # Process PBJ by groups
            for quarter in self.group_dict:
                for facility in self.group_dict[quarter]:
                    paths = self.group_dict[quarter][facility]
                    try:
                        self._process_single_pbj_file(paths, quarter, facility)
                    except Exception as e:
                        print(f"Error processing {Path(paths.pbj_path).name}: {e}")
                        continue

        except Exception as e:
            print(f"Error in process_pbj_files: {e}")
            raise

    def _process_single_pbj_file(self, paths: PathGroup, quarter: str, facility: str):
        """Process a single PBJ file and its corresponding TCLL file."""
        print(f"\nProcessing Standard PBJ for {quarter} {facility}...")

        # Process and export PBJ File
        pbj_file = paths.pbj_path
        pbj_filename = Path(paths.pbj_path).name
        processed_pbj = self.process_pbj(pbj_file)
        
        processed_pbj_path = Path(self.processed_dir) / pbj_filename.replace(".csv", ".xlsx")
        processed_pbj.to_excel(processed_pbj_path, index=False)
        
        # Process and merge TCLL File if found
        if paths.tcll_path:
            tcll_file = paths.tcll_path
            tcll_filename = Path(paths.tcll_path).name
            
            processed_tcll = self.process_tcll(tcll_file)
            processed_tcll_path = Path(self.processed_dir) / tcll_filename
            processed_tcll.to_excel(processed_tcll_path, index=False)

            merged_data = self.merge_tcll_pbj(processed_tcll, processed_pbj)
            merged_path = Path(self.merged_dir) / f"{quarter} {facility} PBJ.xlsx"
            merged_data.to_excel(merged_path, index=False)

        print(f"{quarter} {facility} processed successfully")
        print(f"  - Processed PBJ:  {processed_pbj_path}")
        print(f"  - Processed TCLL: {processed_tcll_path}")
        print(f"  - Merged output:  {merged_path}")
        return

    def process_rehab_pbj_files(self) -> None:
        """Process all Rehab PBJ files."""
        try:
            # Create output directory
            Path(self.rehab_dir).mkdir(parents=True, exist_ok=True)

            for quarter, rehab_files in self.rehab_dict.items():
                for rehab_file in rehab_files:
                    try:
                        self._process_single_rehab_file(rehab_file, quarter)
                    except Exception as e:
                        print(f"Error processing {Path(rehab_file).name}: {e}")
                        continue

        except Exception as e:
            print(f"Error in process_rehab_pbj_files: {e}")
            raise

    def _process_single_rehab_file(self, rehab_file: str, quarter: str) -> bool:
        """Process a single Rehab PBJ file."""
        print(f"\nProcessing Rehab PBJ for {quarter}...")

        # Process the file and split by Facility
        df_rehab_groups = self.process_rehab_pbj(rehab_file, quarter)

        # Export each DataFrame split
        for df_site in df_rehab_groups:
            if df_site.empty:
                continue
            site_name = df_site[self.config.rehab_site_work].iloc[0]
            output_filename = f"{quarter} {site_name} Rehab PBJ.xlsx"
            output_path = Path(self.rehab_dir) / output_filename
            df_site.to_excel(output_path, index=False)
            print(f"  - {output_path}")

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
                    # Convert column to date if column contains date-like data
                    if (df[col].dtype == "object" or 
                        pd.api.types.is_datetime64_any_dtype(df[col])):
                        df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
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
            "acute",
            "post acute",
            "care",
            "center",
            "convalescent hospital",
            "new",
        ]

        for keyword in interchangeable_keywords:
            pattern = r"\b" + re.escape(keyword) + r"\b"
            site = re.sub(pattern, "", site)

        # Collapse extra spaces again after removals
        site = re.sub(r"\s+", " ", site).strip()

        return site

    @staticmethod
    def _match_site_alias(group: str, site_work: str) -> bool:
        """Check if a Group Name (File Name) is an alias or code for a Site Work (Facility Full Name)."""
        site_codes = {
            "CCRC": "Community",
        }
        if (group in site_work or
            (group in site_codes.keys() and site_codes[group] in site_work)):
            return True
        return False


def main():
    """Main function to run the PBJ processing."""
    try:
        processor = PBJProcessor(
            input_dir="input",
            processed_dir="output/processed",
            merged_dir="output/merged",
            rehab_dir="output/rehab",
        )

        # For regular PBJ processing:
        processor.process_pbj_files()

        # For Rehab PBJ processing:
        processor.process_rehab_pbj_files()

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
