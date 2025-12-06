import requests

URL = "http://127.0.0.1:8000/run-agent"
SHEET_ID = "1j2HiLaMGs2jMGZr-sUTv9Fz8AhJk312Y6ZvV3H_c-lQ"
SHEET_NAME = "Sheet1"


tests = [
    "Add a formula in Total column =C2 + D2 + E2.",
    "Create a new column Percentage and add formula =F2/500*100.",
    "Color row 10 with lightblue.",
    "Color column C red.",
    "Color A2:F10 yellow.",
    "Color Category General yellow, SC green, ST purple, OBC blue.",
    "Highlight CGPA green if >9, yellow if 8–9, red if <8.",
    "Insert a new column Remarks at position 3.",
    "Delete the column Email.",
    "Insert a new row at row 5.",
    "Delete row 12.",
    "Move CGPA column to first position.",
    "Rename Category column to Reservation.",
    "Add serial numbers in a column named S.No.",
    "Fill serial numbers in the Rank column.",
    "Fill down the formula in Total column.",
    "Freeze the top 2 rows and first column.",
    "Merge cells A1:E1.",
    "Merge B2:D2 horizontally.",
    "Copy the Name column into a column Full Name Copy.",
    "Copy row 5 to row 20.",
    "Move row 15 to row 3.",
    "Remove all formatting from this sheet.",
    "colour the category column: general yellow, ews orange, obc blue.",
    "sort the sheet using cgpa highest first.",
    "make the rank column serial numbers from 1 to end."
]


print("\n🚀 Starting full automated test suite...\n")

for t in tests:
    print(f"\n🔵 Running: {t}")
    try:
        response = requests.post(
            URL,
            data={
                "prompt": t,
                "sheet_id": SHEET_ID,
                "sheet_name": SHEET_NAME
            }
        )

        if response.status_code == 200:
            print("✅ Success:", response.json())
        else:
            print(f"❌ HTTP {response.status_code}")
            print(response.text)

    except Exception as e:
        print("❌ ERROR:", e)
