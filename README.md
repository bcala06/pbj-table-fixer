# PBJ + TCLL Processing Script

This Python program processes and merges **Time Card by Labor Level (TCLL)** Excel files and **Payroll Based Journal (PBJ)** CSV/Excel files for importing with SimplePBJ.

## Folder Structure

Your working directory should contain the following:

```
.
├── input/
│   ├── 2025 Q3 Facility_Name Time Card by Labor Level.xlsx
│   ├── 2025 Q3 Facility_Name Payroll Based Journal.csv
│   ├── 2025 Q3 Facility_Name Rehab PBJ.xlsx
│   └── Contract Employee ID Master List.xlsx
├── output/
│   ├── processed/
│   ├── merged/
│   └── rehab/
├── main.py
```

- Input files go in the `input/` folder
- Output files will be generated inside `output/processed/`, `output/merged/`, and `output/rehab/`

## File Naming Convention

Correct file names are critical for successful processing:

- **TCLL Excel**: `YYYY QX Facility_Name Time Card by Labor Level.xlsx`
- **PBJ CSV**: `YYYY QX Facility_Name Payroll Based Journal.csv`
- **Rehab PBJ Excel**: `YYYY QX Facility_Name Rehab PBJ.xlsx`
- **Master List Excel**: `Contract Employee ID Master List.xlsx` (Can be any name as long as it contains `"Master List"`.)

## Processing

### Time Card by Labor Level (TCLL)

File: `{Quarter} {Facility_Name} Time Card by Labor Level.xlsx`

Steps:

1. **Labor Distribution**

   - Remove duplicate text separated by `" - "`, keeping the first part only.

2. **Emp #**

   - Convert text to integer (numeric format).

3. **Clock In Type**

   - Only keep rows with `"Clock In"` or `"Work Day Split"`.

4. **Clock Out Type**

   - Only keep rows with `"Clock Out"` or `"Work Day Split"`.

5. **Clock In Time + Clock Out Time**

   - Remove rows where `"Clock In Time" == 8:00 AM` and `"Clock Out Time" == 4:30 PM`

6. **Total Paid**

   - Only keep rows where the value is `>= 8.00`.

7. **Remove**

   - For every 8 hours worked, subtract 0.5 hours: `Remove = floor(Total Paid / 8) * -0.5`

### Payroll Based Journal (PBJ)

File: `{Quarter} {Facility_Name} Payroll Based Journal.csv`

Steps:

1. **File Type**

   - Convert CSV to Excel.

2. **Pay Types Description**

   - Keep only rows where value is `"Work"`.

3. **Labor Distribution**

   - Rename values:  
     `"MDS - RN"` to `"RN with Admin Duties"`  
     `"MDS - LVN"` to `"LVN with Admin Duties"`
   - Remove rows containing:
     - `Physician Assistant`
     - `Occupational Therapist`
     - `Occupational Therapy Assistant`
     - `Occupational Therapy Aide`
     - `Physical Therapist`
     - `Physical Therapy Assistant`
     - `Physical Therapy Aide`
     - `Speech/Language Pathologist`

4. **Merge with Processed TCLL**
   - Append processed TCLL records to PBJ.
   - Align columns; TCLL's `Remove` becomes PBJ's `Hours`.

### Rehab PBJ

- File: `{Quarter} {File_Name} Rehab PBJ.xlsx`
- Requires: `Master List`

Steps:

1. **Employee ID Correction**

   - If `"Site Worked"` and `"Primary Facility"` match:

     - Find the EID from the standard PBJ whose `{Facility_Name}` corresponds to `Site Worked`.

   - If `"Site Worked"` and `"Primary Facility"` do not match:

     - Use the EID from the `Contract Employee ID Master List`.

2. **Split Output**

   - Output one Excel file per `Site Worked` value.

## Output

The program will generate:

- **Processed TCLL/PBJ files**:  
  Saved in `output/processed/`, one `.xlsx` for PBJ and one for TCLL

- **Merged PBJ files**:  
  Saved in `output/merged/`, combining PBJ and TCLL

- **Rehab PBJ files**:  
  Saved in `output/rehab/`, one file per `Site Worked`

## FAQ

- Is the file name tied to your code? Will it need to change next quarter?

  - The only required part of the file names are: `Time Card by Labor Level`, `Payroll Based Journal`, `Rehab PBJ`, and `Master List`.
  - The program will automatically use the provided quarter (formatted as `YYYY QX`) in the filename without having to change the code.
  - To ensure that the output naming conventions are preserved, it is recommended to use the same naming format for subsequent quarters.

- Are the Column positions dependent on your code? What happens if the column changes?

  - The program does **not** depend on the column positions. Instead, the program looks at the **column names** for processing.
  - If the column names were to change in the future. The program will have to be modified to look for the new names.

## Troubleshooting

- Ensure filenames follow the specified patterns.
- Check for renamed or missing columns in the input files.
- If the script fails to find the Master List or cannot decode a file, verify the file format and encoding.

## Notes

- This script assumes consistent formatting across files per quarter and facility.
- All file lookups and exports are relative to the script’s working directory.
