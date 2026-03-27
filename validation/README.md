## Validation Tool

This repository includes a validation tool for verifying the correctness of the VBA processing.

### Overview
- Generates expected results using Python
- Compares results with VBA output
- Outputs comparison summary

---

### Directory Structure
validation/
  python/ # Python scripts (expected value generation)
  test_data/ # Sample CSV files
  vba_test_book.xlsm # Test control workbook

---

### How to Use

1. Open `vba_test_book.xlsm`
2. Set required paths in the "Common Settings" sheet:
   - Python executable path
   - Input folder path
   - Output folder path
3. Click the "Run All" button in the first sheet
4. Python will generate expected results and compare automatically

---

### Notes
- Python environment (numpy, pandas) is required
- Local paths must be configured before execution

---

See `/validation` for details.
