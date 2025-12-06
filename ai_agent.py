import os
import json
from typing import List, Optional, Dict, Any
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from rapidfuzz import process, fuzz
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import re

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]


def a1_to_indexes(a1_range: str):
    """
    Converts A1 notation like 'A2:C10' into (start_row, end_row, start_col, end_col)
    Google API uses zero-based indexes.
    """
    cell_regex = r"([A-Z]+)([0-9]+)"
    start, end = a1_range.split(":")

    def col_to_index(col):
        exp = 0
        val = 0
        for char in reversed(col):
            val += (ord(char) - 64) * (26 ** exp)
            exp += 1
        return val - 1

    start_col, start_row = re.match(cell_regex, start).groups()
    end_col, end_row = re.match(cell_regex, end).groups()

    return (
        int(start_row) - 1,
        int(end_row),
        col_to_index(start_col),
        col_to_index(end_col),
    )


def index_to_col_letter(index: int) -> str:
    """0-based index -> Excel-style column letter"""
    index += 1
    letters = ""
    while index > 0:
        index, rem = divmod(index - 1, 26)
        letters = chr(65 + rem) + letters
    return letters


COLOR_MAP = {
    "red": (1, 0, 0),
    "green": (0, 1, 0),
    "blue": (0, 0, 1),
    "yellow": (1, 1, 0),
    "orange": (1, 0.6, 0),
    "purple": (0.6, 0, 1),
    "pink": (1, 0.6, 0.8),
    "cyan": (0, 1, 1),
    "gray": (0.6, 0.6, 0.6),
    "lightblue": (0.7, 0.85, 1),
}

def add_column_with_serial(service, spreadsheet_id, sheet_name, sheet_id, column_name, position, header_row, num_rows):
    # 1. Insert the column
    add_column(service, spreadsheet_id, sheet_id, position)
    
    # 2. Write the header
    col_letter = index_to_col_letter(position)
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{col_letter}{header_row}",
        valueInputOption="USER_ENTERED",
        body={"values": [[column_name]]},
    ).execute()

    # 3. Fill serial numbers
    values = [[i] for i in range(1, num_rows - header_row + 1)]
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{col_letter}{header_row+1}",
        valueInputOption="USER_ENTERED",
        body={"values": values},
    ).execute()

    return {"status": "done", "column": column_name, "position": position}


def color_multi(service, spreadsheet_id, sheet_id, all_vals, headers, rules):
    requests = []

    for rule in rules:
        col_name = rule["column"]
        value = str(rule["equals"])
        color = rule["color"].lower()
        r, g, b = COLOR_MAP.get(color, (1, 1, 0))

        col_idx = headers.index(col_name)

        for row_index, row in enumerate(all_vals[1:], start=1):
            try:
                if (
                    col_idx < len(row)
                    and str(row[col_idx]).strip().lower() == value.lower()
                ):
                    requests.append(
                        {
                            "repeatCell": {
                                "range": {
                                    "sheetId": sheet_id,
                                    "startRowIndex": row_index,
                                    "endRowIndex": row_index + 1,
                                },
                                "cell": {
                                    "userEnteredFormat": {
                                        "backgroundColor": {
                                            "red": r,
                                            "green": g,
                                            "blue": b,
                                        }
                                    }
                                },
                                "fields": "userEnteredFormat.backgroundColor",
                            }
                        }
                    )
            except Exception:
                pass

    if not requests:
        return {}

    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


# -------------------------
# AUTHENTICATION
# -------------------------
def authenticate_google(
    credentials_path: str = "credentials.json", token_path: str = "token.json"
) -> Any:
    """Authenticate and return Sheets API service"""
    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(credentials_path):
                raise FileNotFoundError(f"{credentials_path} not found")
            flow = InstalledAppFlow.from_client_secrets_file(
                credentials_path, SCOPES
            )
            creds = flow.run_local_server(port=0)

        with open(token_path, "w") as f:
            f.write(creds.to_json())

    return build("sheets", "v4", credentials=creds)


# -------------------------
# SHEET HELPERS
# -------------------------
def get_spreadsheet_metadata(service, spreadsheet_id: str) -> Dict[str, Any]:
    return (
        service.spreadsheets()
        .get(spreadsheetId=spreadsheet_id, includeGridData=False)
        .execute()
    )


def get_sheet_id_by_name(metadata: Dict[str, Any], sheet_name: str) -> Optional[int]:
    for s in metadata.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None


def get_sheet_values(
    service, spreadsheet_id: str, a1_range: str
) -> List[List[Any]]:
    result = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=a1_range)
        .execute()
    )
    return result.get("values", [])


# -------------------------
# LLM SETUP & PARSING
# -------------------------
def setup_llm():
    return ChatGoogleGenerativeAI(model="gemini-2.0-flash", temperature=0)


def parse_instruction_llm(prompt: str, llm, columns: List[str]) -> Dict[str, Any]:
    """Parse natural language into structured action using LLM"""
    system_prompt = f"""
You are an AI instruction parser for Google Sheets.
Available columns: {columns}

Return STRICT JSON ONLY with one of these formats:

SORT:
{{"action": "sort", "column": "<exact column name>", "ascending": true/false}}

MULTI-COLUMN SORT:
{{"action": "multicolumn_sort", "sort": [{{"column": "CGPA", "ascending": false}}, {{"column": "Name", "ascending": true}}]}}

FILTER:
{{"action": "filter", "column": "CGPA", "operator": ">", "value": 8}}

DELETE ROWS:
{{"action": "delete_rows", "column": "CGPA", "operator": "<", "value": 6}}

REMOVE DUPLICATES:
{{"action": "remove_duplicates", "column": "Roll No"}}

ADD FORMULA:
{{"action": "formula", "target_column": "Total", "formula": "=B2+C2+D2"}}

COLOR MULTI:
{{"action": "color_multi",
 "rules": [
    {{"column": "Category", "equals": "General", "color": "yellow"}},
    {{"column": "Category", "equals": "EWS", "color": "red"}},
    {{"column": "Category", "equals": "OBC", "color": "blue"}}
 ]}}

COLOR ROW:
{{"action": "color_row", "row": 5, "color": "yellow"}}

COLOR COLUMN:
{{"action": "color_column", "column": "C", "color": "red"}}

COLOR RANGE:
{{"action": "color_range", "range": "A2:C10", "color": "lightblue"}}

ADD COLUMN:
{{"action": "add_column", "column_name": "Rank", "position": 2}}

DELETE COLUMN:
{{"action": "delete_column", "column": "Category"}}

ADD ROW:
{{"action": "add_row", "position": 10}}

ADD COLUMN WITH SERIAL:
{{"action": "add_column_with_serial", "column_name": "Rank", "position": 0}}
If the user says:
- "add column and fill serial numbers"
- "add a rank column and fill 1..N"
- "add serial column at position X"
- "create rank column"
Then ALWAYS return:
{{"action": "add_column_with_serial", "column_name": "<name>", "position": <index>}}


ADD SERIAL NUMBER INTO A SPECIFIC COLUMN:
{{"action": "add_serial_no", "column_name": "Rank"}}

SPECIAL RULES FOR SERIAL NUMBERS:
- If user says anything like:
  * "add serial numbers"
  * "create rank numbers"
  * "fill 1..N"
  * "generate serial column"
  * "fill serial in this column"
  * "add a rank column and fill it"
  * "add numbering"
  * "create index column"
- Then ALWAYS produce this JSON:
  {{"action": "add_serial_no", "column_name": "<column name>"}}


DELETE ROW:
{{"action": "delete_row", "row": 5}}

MOVE COLUMN:
{{"action": "move_column", "column": "Category", "new_position": 0}}

RENAME COLUMN:
{{"action": "rename_column", "old_name": "Category", "new_name": "Caste"}}

FILL DOWN:
{{"action": "fill_down", "column": "Total"}}

ADD SERIAL NO:
{{"action": "add_serial_no", "column_name": "Serial No"}}

FREEZE:
{{"action": "freeze", "rows": 1, "cols": 0}}

MERGE CELLS:
{{"action": "merge_cells", "range": "A1:C1"}}

COPY COLUMN:
{{"action": "copy_column", "from": "Name", "to": "Name Copy"}}

COPY ROW:
{{"action": "copy_row", "from_row": 5, "to_row": 10}}

MOVE ROW:
{{"action": "move_row", "from_row": 10, "to_row": 3}}

CLEAR FORMATTING:
{{"action": "clear_formatting"}}

COLOR NUMBER RANGE:
{{"action": "color_number_range",
  "column": "CGPA",
  "rules": [
    {{"operator": ">", "value": 9, "color": "green"}},
    {{"operator": "between", "min": 8, "max": 9, "color": "yellow"}},
    {{"operator": "<", "value": 8, "color": "red"}}
  ]
}}

Rules:
- Use EXACT column names from the list
- For descending: ascending=false
- Operators: >, <, >=, <=, =, !=, "between" (with min, max)
- Indices like "position" and "new_position" are ZERO-BASED (0 = first column / row index)
"""

    user_prompt = f"Instruction: {prompt}"

    res = llm.invoke(
        [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ]
    )

    raw = getattr(res, "content", str(res)) or ""
    raw = raw.replace("```json", "").replace("```", "").strip()

    try:
        return json.loads(raw)
    except Exception as e:
        return {"_raw": raw, "_error": str(e)}

def unmerge_all(service, spreadsheet_id, sheet_id):
    body = {
        "requests": [
            {
                "unmergeCells": {
                    "range": {
                        "sheetId": sheet_id
                    }
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()

# -------------------------
# FUZZY MATCHING
# -------------------------
def fuzzy_match_column(target: str, columns: List[str], threshold: float = 60) -> str:
    """Match user input to actual column name using fuzzy matching"""
    if not columns:
        raise ValueError("No columns provided")

    match = process.extractOne(target, columns, scorer=fuzz.WRatio)
    if match and match[1] >= threshold:
        return match[0]

    # Fallback: substring matching
    target_l = target.lower()
    for c in columns:
        if target_l in c.lower() or c.lower() in target_l:
            return c

    return match[0] if match else columns[0]


def ground_columns(
    instruction: Dict[str, Any], actual_columns: List[str]
) -> Dict[str, Any]:
    """Ground LLM-generated column names to actual sheet columns"""
    simple_keys = ["column", "old_name", "from", "to"]
    for key in simple_keys:
        if key in instruction and isinstance(instruction[key], str):
            if instruction[key] not in actual_columns and key != "to":
                # 'to' might be a new column name; don't force if not found
                instruction[key] = fuzzy_match_column(
                    instruction[key], actual_columns
                )

    # ---------- FIX: Auto-select next empty column if target_column is missing ----------
    if "target_column" in instruction:

        # Case 1: target_column is empty or missing → create new column
        if not instruction["target_column"] or instruction["target_column"].strip() == "":
            next_index = len(actual_columns)
            instruction["target_column"] = f"Column_{next_index + 1}"
            instruction["_auto_new_column"] = next_index   # store index for create-column later

        # Case 2: fuzzy match invalid names from LLM
        elif instruction["target_column"] not in actual_columns:
            instruction["target_column"] = fuzzy_match_column(
                instruction["target_column"], actual_columns
            )

# If LLM could not detect target column, auto-create a new one
   
    if instruction.get("action") == "multicolumn_sort":
        for sort_spec in instruction.get("sort", []):
            if (
                isinstance(sort_spec.get("column"), str)
                and sort_spec["column"] not in actual_columns
            ):
                sort_spec["column"] = fuzzy_match_column(
                    sort_spec["column"], actual_columns
                )

    # Ground any nested rules that contain "column"
    if "rules" in instruction and isinstance(instruction["rules"], list):
        for r in instruction["rules"]:
            if isinstance(r, dict) and "column" in r:
                if r["column"] not in actual_columns:
                    r["column"] = fuzzy_match_column(r["column"], actual_columns)

    return instruction


# -------------------------
# SHEET OPERATIONS
# -------------------------
def apply_sort(
    service,
    spreadsheet_id: str,
    sheet_id: int,
    col_index: int,
    start_row: int,
    end_row: int,
    num_cols: int,
    ascending: bool = True,
):
    """Sort single column"""
    body = {
        "requests": [
            {
                "sortRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "sortSpecs": [
                        {
                            "dimensionIndex": col_index,
                            "sortOrder": "ASCENDING"
                            if ascending
                            else "DESCENDING",
                        }
                    ],
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def apply_multi_sort(
    service,
    spreadsheet_id: str,
    sheet_id: int,
    sort_specs: List[Dict],
    start_row: int,
    end_row: int,
    num_cols: int,
):
    """Sort multiple columns"""
    specs = []
    for s in sort_specs:
        specs.append(
            {
                "dimensionIndex": s["col_index"],
                "sortOrder": "ASCENDING" if s["ascending"] else "DESCENDING",
            }
        )

    body = {
        "requests": [
            {
                "sortRange": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": 0,
                        "endColumnIndex": num_cols,
                    },
                    "sortSpecs": specs,
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def apply_filter(
    service,
    spreadsheet_id: str,
    sheet_id: int,
    col_index: int,
    operator: str,
    value,
):
    """Apply filter to column"""
    condition_map = {
        ">": "NUMBER_GREATER",
        "<": "NUMBER_LESS",
        "=": "NUMBER_EQ",
        "!=": "NUMBER_NOT_EQ",
        ">=": "NUMBER_GREATER_THAN_EQ",
        "<=": "NUMBER_LESS_THAN_EQ",
    }

    body = {
        "requests": [
            {
                "setBasicFilter": {
                    "filter": {
                        "range": {"sheetId": sheet_id},
                        "criteria": {
                            str(col_index): {
                                "condition": {
                                    "type": condition_map.get(
                                        operator, "NUMBER_EQ"
                                    ),
                                    "values": [
                                        {"userEnteredValue": str(value)}
                                    ],
                                }
                            }
                        },
                    }
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def delete_rows_batch(
    service, spreadsheet_id: str, sheet_id: int, row_indices: List[int]
):
    """Delete multiple rows (sorted in reverse to avoid index shifts)"""
    requests = []
    for idx in sorted(row_indices, reverse=True):
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": idx,
                        "endIndex": idx + 1,
                    }
                }
            }
        )

    if not requests:
        return {}

    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def remove_duplicates(service, spreadsheet_id, sheet_id, col_index):
    body = {
        "requests": [
            {
                "deleteDuplicates": {
                    "range": {
                        "sheetId": sheet_id
                    },
                    "comparisonColumns": [
                        {
                            "sheetId": sheet_id,
                            "dimension": "COLUMNS",
                            "startIndex": col_index,
                            "endIndex": col_index + 1
                        }
                    ]
                }
            }
        ]
    }

    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=body
    ).execute()


def add_formula(
    service, spreadsheet_id: str, sheet_name: str, target_column: str, formula: str
):
    """Add formula to target column"""
    body = {"values": [[formula]]}
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!{target_column}2",
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )


def color_range(service, spreadsheet_id, sheet_id, a1_range, red, green, blue):
    start_row, end_row, start_col, end_col = a1_to_indexes(a1_range)

    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": red,
                                "green": green,
                                "blue": blue,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        ]
    }

    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def color_row(service, spreadsheet_id, sheet_id, row, r, g, b):
    a1 = f"{row}:{row}"
    return color_range(service, spreadsheet_id, sheet_id, a1, r, g, b)


def color_column(service, spreadsheet_id, sheet_id, column_letter, r, g, b):
    a1 = f"{column_letter}:{column_letter}"
    return color_range(service, spreadsheet_id, sheet_id, a1, r, g, b)


def color_if(service, spreadsheet_id, sheet_id, rows, r, g, b):
    requests = []

    for row_index in rows:
        requests.append(
            {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_index,
                        "endRowIndex": row_index + 1,
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": r,
                                "green": g,
                                "blue": b,
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor",
                }
            }
        )

    if not requests:
        return {}

    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def add_column(service, spreadsheet_id: str, sheet_id: int, index: int):
    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    },
                    "inheritFromBefore": False  # FIXED
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def delete_column(service, spreadsheet_id: str, sheet_id: int, index: int):
    body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    }
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def add_row(service, spreadsheet_id: str, sheet_id: int, index: int):
    body = {
        "requests": [
            {
                "insertDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    },
                    "inheritFromBefore": True,
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def delete_row(service, spreadsheet_id: str, sheet_id: int, index: int):
    body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": index,
                        "endIndex": index + 1,
                    }
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def move_column(service, spreadsheet_id, sheet_id, old_index, new_index):
    body = {
        "requests": [
            {
                "moveDimension": {
                    "source": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": old_index,
                        "endIndex": old_index + 1,
                    },
                    "destinationIndex": new_index,
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def rename_column(
    service,
    spreadsheet_id,
    sheet_name: str,
    header_row: int,
    col_index: int,
    new_name: str,
):
    col_letter = index_to_col_letter(col_index)
    body = {"values": [[new_name]]}
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!{col_letter}{header_row}",
            valueInputOption="USER_ENTERED",
            body=body,
        )
        .execute()
    )


def fill_down_column(
    service,
    spreadsheet_id,
    sheet_name: str,
    col_index: int,
    header_row: int,
    num_rows: int,
):
    """Copy the formula/value from first data row to all below rows in that column."""
    if num_rows <= header_row + 1:
        return {}

    col_letter = index_to_col_letter(col_index)
    first_data_row = header_row + 1

    # Get the formula/value in the first data cell
    source_range = f"{sheet_name}!{col_letter}{first_data_row}"
    src_vals = get_sheet_values(service, spreadsheet_id, source_range)
    if not src_vals or not src_vals[0]:
        return {}

    formula_or_value = src_vals[0][0]

    # Prepare values for all remaining rows
    target_start_row = first_data_row + 1
    if target_start_row > num_rows:
        return {}

    target_range = (
        f"{sheet_name}!{col_letter}{target_start_row}:{col_letter}{num_rows}"
    )
    values = [[formula_or_value] for _ in range(target_start_row, num_rows + 1)]

    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=target_range,
            valueInputOption="USER_ENTERED",
            body={"values": values},
        )
        .execute()
    )


def add_serial_no_column(
    service,
    spreadsheet_id,
    sheet_name: str,
    headers: List[str],
    header_row: int,
    num_rows: int,
    column_name: str,
    sheet_id: int,
):
    """Ensure a serial number column exists and fill 1..N-1."""
    # If column exists, use it; else insert at position 0
    if column_name in headers:
        col_index = headers.index(column_name)
    else:
        col_index = 0
        add_column(service, spreadsheet_id, sheet_id, col_index)
        # Update header cell
        col_letter = index_to_col_letter(col_index)
        (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!{col_letter}{header_row}",
                valueInputOption="USER_ENTERED",
                body={"values": [[column_name]]},
            )
            .execute()
        )

    # Fill serial numbers from header_row+1 onwards
    if num_rows <= header_row:
        return {}

    col_letter = index_to_col_letter(col_index)
    start_row = header_row + 1
    count = num_rows - header_row
    values = [[i] for i in range(1, count + 1)]

    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_name}!{col_letter}{start_row}",
            valueInputOption="USER_ENTERED",
            body={"values": values},
        )
        .execute()
    )


def freeze_panes(service, spreadsheet_id, sheet_id, rows: int, cols: int):
    body = {
        "requests": [
            {
                "updateSheetProperties": {
                    "properties": {
                        "sheetId": sheet_id,
                        "gridProperties": {
                            "frozenRowCount": rows,
                            "frozenColumnCount": cols,
                        },
                    },
                    "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount",
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def merge_cells(
    service, spreadsheet_id, sheet_id, a1_range: str, merge_type: str = "MERGE_ALL"
):
    start_row, end_row, start_col, end_col = a1_to_indexes(a1_range)
    body = {
        "requests": [
            {
                "mergeCells": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row,
                        "endRowIndex": end_row,
                        "startColumnIndex": start_col,
                        "endColumnIndex": end_col,
                    },
                    "mergeType": merge_type,
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def copy_column_values(
    service,
    spreadsheet_id,
    sheet_name: str,
    headers: List[str],
    from_name: str,
    to_name: str,
    header_row: int,
    num_rows: int,
    sheet_id: int,
):
    # Source column index
    if from_name not in headers:
        raise RuntimeError(f"Source column '{from_name}' not found")
    from_idx = headers.index(from_name)

    # Target column: if not exist, create after source
    if to_name in headers:
        to_idx = headers.index(to_name)
    else:
        to_idx = from_idx + 1
        add_column(service, spreadsheet_id, sheet_id, to_idx)
        col_letter_to = index_to_col_letter(to_idx)
        (
            service.spreadsheets()
            .values()
            .update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!{col_letter_to}{header_row}",
                valueInputOption="USER_ENTERED",
                body={"values": [[to_name]]},
            )
            .execute()
        )

    from_letter = index_to_col_letter(from_idx)
    to_letter = index_to_col_letter(to_idx)

    if num_rows <= header_row:
        return {}

    start_row = header_row + 1
    range_from = (
        f"{sheet_name}!{from_letter}{start_row}:{from_letter}{num_rows}"
    )
    vals = get_sheet_values(service, spreadsheet_id, range_from)
    if not vals:
        vals = [[] for _ in range(start_row, num_rows + 1)]

    range_to = f"{sheet_name}!{to_letter}{start_row}:{to_letter}{num_rows}"
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=range_to,
            valueInputOption="USER_ENTERED",
            body={"values": vals},
        )
        .execute()
    )


def copy_row_values(
    service, spreadsheet_id, sheet_name: str, from_row: int, to_row: int
):
    from_range = f"{sheet_name}!{from_row}:{from_row}"
    vals = get_sheet_values(service, spreadsheet_id, from_range)
    if not vals:
        return {}

    to_range = f"{sheet_name}!{to_row}:{to_row}"
    return (
        service.spreadsheets()
        .values()
        .update(
            spreadsheetId=spreadsheet_id,
            range=to_range,
            valueInputOption="USER_ENTERED",
            body={"values": vals},
        )
        .execute()
    )


def move_row_dimension(
    service, spreadsheet_id, sheet_id, from_index: int, to_index: int
):
    body = {
        "requests": [
            {
                "moveDimension": {
                    "source": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": from_index,
                        "endIndex": from_index + 1,
                    },
                    "destinationIndex": to_index,
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def clear_formatting(service, spreadsheet_id, sheet_id):
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {"sheetId": sheet_id},
                    "cell": {"userEnteredFormat": {}},
                    "fields": "userEnteredFormat",
                }
            }
        ]
    }
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()


def color_number_range(
    service,
    spreadsheet_id,
    sheet_id,
    headers: List[str],
    all_vals: List[List[Any]],
    column_name: str,
    rules: List[Dict[str, Any]],
):
    """Apply color based on numeric ranges in a column."""
    if column_name not in headers:
        raise RuntimeError(f"Column '{column_name}' not found")
    col_idx = headers.index(column_name)

    requests = []

    for row_idx, row in enumerate(all_vals[1:], start=1):
        if col_idx >= len(row):
            continue
        try:
            v = float(str(row[col_idx]).strip())
        except Exception:
            continue

        applied_color = None
        for rule in rules:
            op = rule.get("operator")
            color_name = rule.get("color", "yellow").lower()
            if op == "between":
                mn = float(rule["min"])
                mx = float(rule["max"])
                if mn <= v <= mx:
                    applied_color = color_name
                    break
            else:
                val = float(rule.get("value", 0))
                if op == ">" and v > val:
                    applied_color = color_name
                    break
                elif op == ">=" and v >= val:
                    applied_color = color_name
                    break
                elif op == "<" and v < val:
                    applied_color = color_name
                    break
                elif op == "<=" and v <= val:
                    applied_color = color_name
                    break
                elif op == "=" and v == val:
                    applied_color = color_name
                    break
                elif op == "!=" and v != val:
                    applied_color = color_name
                    break

        if applied_color:
            r, g, b = COLOR_MAP.get(applied_color, (1, 1, 0))
            requests.append(
                {
                    "repeatCell": {
                        "range": {
                            "sheetId": sheet_id,
                            "startRowIndex": row_idx,
                            "endRowIndex": row_idx + 1,
                            "startColumnIndex": col_idx,
                            "endColumnIndex": col_idx + 1,
                        },
                        "cell": {
                            "userEnteredFormat": {
                                "backgroundColor": {
                                    "red": r,
                                    "green": g,
                                    "blue": b,
                                }
                            }
                        },
                        "fields": "userEnteredFormat.backgroundColor",
                    }
                }
            )

    if not requests:
        return {}

    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id, body=body
    ).execute()
def index_to_column_letter(index):
    letters = ""
    while index >= 0:
        letters = chr(index % 26 + 65) + letters
        index = index // 26 - 1
    return letters


# -------------------------
# MAIN AGENT CLASS
# -------------------------
class SheetsAIAgent:
    def __init__(
        self, credentials_path: str = "credentials.json", token_path: str = "token.json"
    ):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        self.llm = setup_llm()

    def connect(self):
        """Connect to Google Sheets API"""
        self.service = authenticate_google(self.credentials_path, self.token_path)

    def execute(
        self,
        spreadsheet_id: str,
        sheet_name: str,
        user_prompt: str,
        header_row: int = 1,
    ):
        """
        Main execution method - handles all operations
        """
        if not self.service:
            self.connect()

        # Get headers (must use A1 notation, row-only ranges are invalid)
        # get actual column count
        # --------------------------------------
# FETCH METADATA FIRST (NEEDED FOR HEADERS)
        # --------------------------------------
        metadata = get_spreadsheet_metadata(self.service, spreadsheet_id)
        sheet_id = get_sheet_id_by_name(metadata, sheet_name)
        if sheet_id is None:
            raise RuntimeError(f"Sheet '{sheet_name}' not found")

        # Get actual column count of the sheet (prevents Google API 500 crash)
        sheet_props = None
        for sheet in metadata.get("sheets", []):
            if sheet["properties"]["title"] == sheet_name:
                sheet_props = sheet["properties"]["gridProperties"]
                break

        if sheet_props is None:
            raise RuntimeError("Failed to read sheet properties")

        col_count = sheet_props.get("columnCount", 26)

        # Convert column index → Excel letter (0=A, 25=Z, 26=AA, ...)
        def index_to_column_letter(index):
            letters = ""
            while index >= 0:
                letters = chr(index % 26 + 65) + letters
                index = index // 26 - 1
            return letters

        last_col_letter = index_to_column_letter(col_count - 1)

        # SAFE HEADER RANGE (NO MORE INTERNAL ERRORS)
        header_a1 = f"{sheet_name}!A{header_row}:{last_col_letter}{header_row}"

        headers_vals = get_sheet_values(self.service, spreadsheet_id, header_a1)
        if not headers_vals or len(headers_vals) == 0:
            raise RuntimeError("Could not read header row")


        headers = headers_vals[0]

        # Get metadata
        metadata = get_spreadsheet_metadata(self.service, spreadsheet_id)
        sheet_id = get_sheet_id_by_name(metadata, sheet_name)
        if sheet_id is None:
            raise RuntimeError(f"Sheet '{sheet_name}' not found")

        # Parse instruction with LLM
        instruction = parse_instruction_llm(user_prompt, self.llm, headers)

        if "_error" in instruction:
            return {
                "status": "error",
                "message": "Failed to parse instruction",
                "details": instruction,
            }

        # Ground column names to actual headers
        instruction = ground_columns(instruction, headers)

        # Get all data for operations that need it
        all_vals = get_sheet_values(self.service, spreadsheet_id, f"{sheet_name}")
        num_rows = len(all_vals)
        num_cols = len(headers)

        action = instruction.get("action")
        result: Dict[str, Any] = {}

        # SORT
        if action == "sort":
            col_name = instruction["column"]
            col_idx = headers.index(col_name)
            ascending = instruction.get("ascending", True)

            result = apply_sort(
                self.service,
                spreadsheet_id,
                sheet_id,
                col_idx,
                header_row,
                num_rows,
                num_cols,
                ascending,
            )
            return {
                "status": "success",
                "action": "sort",
                "column": col_name,
                "ascending": ascending,
                "response": result,
            }

        # MULTI-COLUMN SORT
        elif action == "multicolumn_sort":
            sort_specs = []
            for s in instruction.get("sort", []):
                col_idx = headers.index(s["column"])
                sort_specs.append(
                    {"col_index": col_idx, "ascending": s.get("ascending", True)}
                )

            result = apply_multi_sort(
                self.service,
                spreadsheet_id,
                sheet_id,
                sort_specs,
                header_row,
                num_rows,
                num_cols,
            )
            return {
                "status": "success",
                "action": "multicolumn_sort",
                "columns": [s["column"] for s in instruction["sort"]],
                "response": result,
            }

        # FILTER
        elif action == "filter":
            col_idx = headers.index(instruction["column"])
            result = apply_filter(
                self.service,
                spreadsheet_id,
                sheet_id,
                col_idx,
                instruction["operator"],
                instruction["value"],
            )
            return {
                "status": "success",
                "action": "filter",
                "column": instruction["column"],
                "operator": instruction["operator"],
                "value": instruction["value"],
                "response": result,
            }

        # DELETE ROWS (by numeric condition)
        elif action == "delete_rows":
            col_name = instruction["column"]
            col_idx = headers.index(col_name)
            op = instruction["operator"]
            val = float(instruction["value"])

            delete_indices = []
            for i, row in enumerate(all_vals[1:], start=1):
                try:
                    if col_idx >= len(row):
                        continue
                    cell_val = float(row[col_idx])
                    if (op == "<" and cell_val < val) or \
                       (op == ">" and cell_val > val) or \
                       (op == "=" and cell_val == val) or \
                       (op == "!=" and cell_val != val) or \
                       (op == "<=" and cell_val <= val) or \
                       (op == ">=" and cell_val >= val):
                        delete_indices.append(i)
                except Exception:
                    pass

            if delete_indices:
                result = delete_rows_batch(
                    self.service, spreadsheet_id, sheet_id, delete_indices
                )

            return {
                "status": "success",
                "action": "delete_rows",
                "column": col_name,
                "deleted_count": len(delete_indices),
                "response": result,
            }

        # REMOVE DUPLICATES
        elif action == "remove_duplicates":
            col_idx = headers.index(instruction["column"])
            try:
                result = remove_duplicates(self.service, spreadsheet_id, sheet_id, col_idx)
            except HttpError as err:
                if "merged cell" in str(err):
                    unmerge_all(self.service, spreadsheet_id, sheet_id)
                    result = remove_duplicates(self.service, spreadsheet_id, sheet_id, col_idx)
                else:
                    raise err    
            return {
                "status": "success",
                "action": "remove_duplicates",
                "column": instruction["column"],
                "response": result,
            }

        # ADD FORMULA
        elif action == "formula":

            # 1️⃣ Auto-create column if needed
            if instruction.get("_auto_new_column") is not None:
                new_index = instruction["_auto_new_column"]
                add_column(self.service, spreadsheet_id, sheet_id, new_index)

                col_letter = index_to_col_letter(new_index)
                self.service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!{col_letter}{header_row}",
                    valueInputOption="USER_ENTERED",
                    body={"values": [[instruction["target_column"]]]},
                ).execute()

                headers.append(instruction["target_column"])

            # 2️⃣ Now apply formula
            target_col = instruction["target_column"]
            formula = instruction["formula"]

            # Write formula to row 2
            col_idx = headers.index(target_col)
            col_letter = index_to_col_letter(col_idx)

            self.service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!{col_letter}{header_row+1}",
                valueInputOption="USER_ENTERED",
                body={"values": [[formula]]},
            ).execute()

            # 3️⃣ Optional: fill down
            fill_down_column(
                self.service,
                spreadsheet_id,
                sheet_name,
                col_idx,
                header_row,
                num_rows
            )

            return {
                "status": "success",
                "action": "formula",
                "target_column": target_col,
                "formula": formula,
            }

        # COLOR ROW
        elif action == "color_row":
            row = instruction["row"]
            color = instruction["color"].lower()
            r, g, b = COLOR_MAP.get(color, (1, 1, 0))  # default yellow
            result = color_row(self.service, spreadsheet_id, sheet_id, row, r, g, b)
            return {
                "status": "success",
                "action": "color_row",
                "row": row,
                "color": color,
                "response": result,
            }

        # COLOR COLUMN
        elif action == "color_column":
            col = instruction["column"]
            color = instruction["color"].lower()
            r, g, b = COLOR_MAP.get(color, (1, 1, 0))
            result = color_column(
                self.service, spreadsheet_id, sheet_id, col, r, g, b
            )
            return {
                "status": "success",
                "action": "color_column",
                "column": col,
                "color": color,
                "response": result,
            }

        # COLOR RANGE
        elif action == "color_range":
            a1 = instruction["range"]
            color = instruction["color"].lower()
            r, g, b = COLOR_MAP.get(color, (1, 1, 0))
            result = color_range(
                self.service, spreadsheet_id, sheet_id, a1, r, g, b
            )
            return {
                "status": "success",
                "action": "color_range",
                "range": a1,
                "color": color,
                "response": result,
            }

        # COLOR IF (single value)
        elif action == "color_if":
            target_col = instruction["column"]
            val = str(instruction["equals"])
            color = instruction["color"].lower()
            r, g, b = COLOR_MAP.get(color, (1, 1, 0))

            col_idx = headers.index(target_col)

            matching_rows = []
            for i, row in enumerate(all_vals[1:], start=1):
                try:
                    if col_idx < len(row) and str(row[col_idx]).strip().lower() == val.lower():
                        matching_rows.append(i)
                except Exception:
                    pass

            result = color_if(
                self.service, spreadsheet_id, sheet_id, matching_rows, r, g, b
            )

            return {
                "status": "success",
                "action": "color_if",
                "column": target_col,
                "equals": val,
                "color": color,
                "rows_colored": matching_rows,
                "response": result,
            }

        # COLOR MULTI (multiple values -> different colors)
        elif action == "color_multi":
            result = color_multi(
                self.service, spreadsheet_id, sheet_id, all_vals, headers, instruction["rules"]
            )
            return {
                "status": "success",
                "action": "color_multi",
                "rules": instruction["rules"],
                "response": result,
            }

        # ADD COLUMN
        elif action == "add_column":
            col_name = instruction["column_name"]
            pos = int(instruction["position"])
            result = add_column(self.service, spreadsheet_id, sheet_id, pos)
            col_letter = index_to_col_letter(pos)
            (
                self.service.spreadsheets()
                .values()
                .update(
                    spreadsheetId=spreadsheet_id,
                    range=f"{sheet_name}!{col_letter}{header_row}",
                    valueInputOption="USER_ENTERED",
                    body={"values": [[col_name]]},
                )
                .execute()
            )
            return {
                "status": "success",
                "action": "add_column",
                "column_name": col_name,
                "position": pos,
                "response": result,
            }

        # DELETE COLUMN
        elif action == "delete_column":
            col_name = instruction["column"]
            index = headers.index(col_name)
            result = delete_column(self.service, spreadsheet_id, sheet_id, index)
            return {
                "status": "success",
                "action": "delete_column",
                "column": col_name,
                "response": result,
            }

        # ADD ROW
        elif action == "add_row":
            pos = int(instruction["position"])
            result = add_row(self.service, spreadsheet_id, sheet_id, pos)
            return {
                "status": "success",
                "action": "add_row",
                "position": pos,
                "response": result,
            }

        # DELETE ROW
        elif action == "delete_row":
            row = int(instruction["row"])
            result = delete_row(self.service, spreadsheet_id, sheet_id, row)
            return {
                "status": "success",
                "action": "delete_row",
                "row": row,
                "response": result,
            }

        # MOVE COLUMN
        elif action == "move_column":
            col_name = instruction["column"]
            old_index = headers.index(col_name)
            new_index = int(instruction["new_position"])
            result = move_column(
                self.service, spreadsheet_id, sheet_id, old_index, new_index
            )
            return {
                "status": "success",
                "action": "move_column",
                "column": col_name,
                "from_index": old_index,
                "to_index": new_index,
                "response": result,
            }

        # RENAME COLUMN
        elif action == "rename_column":
            old_name = instruction["old_name"]
            new_name = instruction["new_name"]
            col_idx = headers.index(old_name)
            result = rename_column(
                self.service,
                spreadsheet_id,
                sheet_name,
                header_row,
                col_idx,
                new_name,
            )
            return {
                "status": "success",
                "action": "rename_column",
                "old_name": old_name,
                "new_name": new_name,
                "response": result,
            }

        # FILL DOWN
        elif action == "fill_down":
            col_name = instruction["column"]
            col_idx = headers.index(col_name)
            result = fill_down_column(
                self.service,
                spreadsheet_id,
                sheet_name,
                col_idx,
                header_row,
                num_rows,
            )
            return {
                "status": "success",
                "action": "fill_down",
                "column": col_name,
                "response": result,
            }

        # ADD SERIAL NO
        elif action == "add_serial_no":
            col_name = instruction.get("column_name", "Serial No")
            result = add_serial_no_column(
                self.service,
                spreadsheet_id,
                sheet_name,
                headers,
                header_row,
                num_rows,
                col_name,
                sheet_id,
            )
            return {
                "status": "success",
                "action": "add_serial_no",
                "column_name": col_name,
                "response": result,
            }

        # FREEZE
        elif action == "freeze":
            rows = int(instruction.get("rows", 1))
            cols = int(instruction.get("cols", 0))
            result = freeze_panes(
                self.service, spreadsheet_id, sheet_id, rows, cols
            )
            return {
                "status": "success",
                "action": "freeze",
                "rows": rows,
                "cols": cols,
                "response": result,
            }

        # MERGE CELLS
        elif action == "merge_cells":
            a1 = instruction["range"]
            merge_type = instruction.get("merge_type", "MERGE_ALL")
            result = merge_cells(
                self.service, spreadsheet_id, sheet_id, a1, merge_type
            )
            return {
                "status": "success",
                "action": "merge_cells",
                "range": a1,
                "merge_type": merge_type,
                "response": result,
            }

        # COPY COLUMN
        elif action == "copy_column":
            from_name = instruction["from"]
            to_name = instruction["to"]
            result = copy_column_values(
                self.service,
                spreadsheet_id,
                sheet_name,
                headers,
                from_name,
                to_name,
                header_row,
                num_rows,
                sheet_id,
            )
            return {
                "status": "success",
                "action": "copy_column",
                "from": from_name,
                "to": to_name,
                "response": result,
            }

        # COPY ROW
        elif action == "copy_row":
            from_row = int(instruction["from_row"])
            to_row = int(instruction["to_row"])
            result = copy_row_values(
                self.service, spreadsheet_id, sheet_name, from_row, to_row
            )
            return {
                "status": "success",
                "action": "copy_row",
                "from_row": from_row,
                "to_row": to_row,
                "response": result,
            }

        # MOVE ROW
        elif action == "move_row":
            from_row = int(instruction["from_row"])
            to_row = int(instruction["to_row"])
            # These are indices, not A1 row numbers; if user gives A1 rows,
            # you may want to subtract 1 – but here we treat them as 0-based per prompt rules.
            result = move_row_dimension(
                self.service, spreadsheet_id, sheet_id, from_row, to_row
            )
            return {
                "status": "success",
                "action": "move_row",
                "from_row": from_row,
                "to_row": to_row,
                "response": result,
            }

        # CLEAR FORMATTING
        elif action == "clear_formatting":
            unmerge_all(self.service, spreadsheet_id, sheet_id)
            result = clear_formatting(self.service, spreadsheet_id, sheet_id)
            return {
                "status": "success",
                "action": "clear_formatting",
                "response": result,
            }

        # COLOR NUMBER RANGE
        elif action == "color_number_range":
            col_name = instruction["column"]
            rules = instruction.get("rules", [])
            result = color_number_range(
                self.service,
                spreadsheet_id,
                sheet_id,
                headers,
                all_vals,
                col_name,
                rules,
            )
            return {
                "status": "success",
                "action": "color_number_range",
                "column": col_name,
                "rules": rules,
                "response": result,
            }
        elif action == "add_column_with_serial":
            col_name = instruction["column_name"]
            pos = int(instruction["position"])

            result = add_column_with_serial(
                self.service,
                spreadsheet_id,
                sheet_name,
                sheet_id,
                col_name,
                pos,
                header_row,
                num_rows
            )

            return {
                "status": "success",
                "action": "add_column_with_serial",
                "column_name": col_name,
                "position": pos,
                "response": result
            }

        else:
            return {"status": "error", "message": f"Unknown action: {action}"}


# -------------------------
# CLI INTERFACE
# -------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Google Sheets AI Agent")
    parser.add_argument(
        "--spreadsheet_id", required=True, help="Spreadsheet ID from URL"
    )
    parser.add_argument("--sheet_name", default="Sheet1", help="Sheet name")
    parser.add_argument(
        "--prompt", required=True, help="Natural language instruction"
    )
    parser.add_argument(
        "--credentials", default="credentials.json", help="OAuth credentials path"
    )
    parser.add_argument(
        "--token", default="token.json", help="Token path"
    )

    args = parser.parse_args()

    agent = SheetsAIAgent(
        credentials_path=args.credentials, token_path=args.token
    )
    result = agent.execute(args.spreadsheet_id, args.sheet_name, args.prompt)

    print(json.dumps(result, indent=2))
