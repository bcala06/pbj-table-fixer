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

## 📝 File Naming Convention

Correct file names are critical for successful processing:

- **TCLL Excel**: `YYYY QX Facility_Name Time Card by Labor Level.xlsx`
- **PBJ CSV**: `YYYY QX Facility_Name Payroll Based Journal.csv`
- **Rehab PBJ Excel**: `YYYY QX Facility_Name Rehab PBJ.xlsx`
- **Master List Excel**: Should include “Master List” in the filename (e.g., `Contract Employee ID Master List.xlsx`)

## ⚙️ Processing

### 🕒 Time Card by Labor Level (TCLL)

File: `{Quarter} {Facility} Time Card by Labor Level.xlsx`

Steps:

1. **Labor Distribution (Column C)**

   - Remove duplicate text separated by `" - "`, keeping the first part only.

2. **Emp # (Column D)**

   - Convert text to integer (numeric format).

3. **Clock In Type (Column H)**

   - Keep only rows with `"Clock In"` or `"Work Day Split"`.

4. **Clock Out Type (Column J)**

   - Keep only rows with `"Clock Out"` or `"Work Day Split"`.

5. **Total Paid (Column N)**

   - Filter out rows where value is `≤ 8.00`.

6. **Remove (Column P)**
   - For every 8 hours worked, subtract 0.5 hours:  
     `Remove = floor(Total Paid / 8) * -0.5`

### 📊 Payroll Based Journal (PBJ)

File: `{Quarter} {Facility} Payroll Based Journal.csv`

Steps:

1. **File Type**

   - Convert CSV to Excel.

2. **Pay Types Description (Column F)**

   - Keep only rows where value is `"Work"`.

3. **Labor Distribution (Column L)**

   - Rename values:  
     `"MDS - RN"` to `"RN with Admin Duties"`  
     `"LVN - RN"` to `"LVN with Admin Duties"`
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

### 🏥 Rehab PBJ

File: `{Quarter} {Facility} Rehab PBJ.xlsx`
Requires: `Master List Excel`

Steps:

1. **Employee ID Correction**

   - If `Site Worked` and `Primary Facility` match:
     - Use the original Employee number.
   - If `Site Worked` and `Primary Facility` do not match:
     - Use the EID from the Master List instead.

2. **Split Output**
   - Output one Excel file per `Site Worked` value.

## 📤 Output

The program will generate:

- ✅ **Processed files**:  
  Saved in `output/processed/`, one `.xlsx` for PBJ and one for TCLL

- ✅ **Merged files**:  
  Saved in `output/merged/`, combining PBJ and TCLL

- ✅ **Rehab outputs**:  
  Saved in `output/rehab/`, one file per `Site Worked`

## 🛠 Troubleshooting

- Ensure filenames follow the specified patterns.
- Check for renamed or missing columns in the input files.
- If the script fails to find the Master List or cannot decode a file, verify the file format and encoding.

## 🔒 Notes

- This script assumes consistent formatting across files per quarter and facility.
- All file lookups and exports are relative to the script’s working directory.
