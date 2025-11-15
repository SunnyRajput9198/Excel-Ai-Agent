import os
import json
from typing import List, Optional, Dict, Any
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from rapidfuzz import process, fuzz
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/spreadsheets"]

# -------------------------
# AUTHENTICATION
# -------------------------
def authenticate_google(credentials_path: str = "credentials.json", token_path: str = "token.json") -> Any:
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
            flow = InstalledAppFlow.from_client_secrets_file(credentials_path, SCOPES)
            creds = flow.run_local_server(port=0)
        
        with open(token_path, "w") as f:
            f.write(creds.to_json())
    
    return build("sheets", "v4", credentials=creds)

# -------------------------
# SHEET HELPERS
# -------------------------
def get_spreadsheet_metadata(service, spreadsheet_id: str) -> Dict[str, Any]:
    return service.spreadsheets().get(spreadsheetId=spreadsheet_id, includeGridData=False).execute()

def get_sheet_id_by_name(metadata: Dict[str, Any], sheet_name: str) -> Optional[int]:
    for s in metadata.get("sheets", []):
        props = s.get("properties", {})
        if props.get("title") == sheet_name:
            return props.get("sheetId")
    return None

def get_sheet_values(service, spreadsheet_id: str, a1_range: str) -> List[List[Any]]:
    result = service.spreadsheets().values().get(spreadsheetId=spreadsheet_id, range=a1_range).execute()
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

Rules:
- Use EXACT column names from the list
- For descending: ascending=false
- Operators: >, <, =, !=
"""
    
    user_prompt = f"Instruction: {prompt}"
    
    res = llm.invoke([
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt}
    ])
    
    raw = getattr(res, "content", str(res)) or ""
    raw = raw.replace("```json", "").replace("```", "").strip()
    
    try:
        return json.loads(raw)
    except Exception as e:
        return {"_raw": raw, "_error": str(e)}

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

def ground_columns(instruction: Dict[str, Any], actual_columns: List[str]) -> Dict[str, Any]:
    """Ground LLM-generated column names to actual sheet columns"""
    if "column" in instruction:
        if instruction["column"] not in actual_columns:
            instruction["column"] = fuzzy_match_column(instruction["column"], actual_columns)
    
    if "target_column" in instruction:
        if instruction["target_column"] not in actual_columns:
            instruction["target_column"] = fuzzy_match_column(instruction["target_column"], actual_columns)
    
    if instruction.get("action") == "multicolumn_sort":
        for sort_spec in instruction.get("sort", []):
            if sort_spec["column"] not in actual_columns:
                sort_spec["column"] = fuzzy_match_column(sort_spec["column"], actual_columns)
    
    return instruction

# -------------------------
# SHEET OPERATIONS
# -------------------------
def apply_sort(service, spreadsheet_id: str, sheet_id: int, col_index: int, 
               start_row: int, end_row: int, num_cols: int, ascending: bool = True):
    """Sort single column"""
    body = {
        "requests": [{
            "sortRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
                },
                "sortSpecs": [{
                    "dimensionIndex": col_index,
                    "sortOrder": "ASCENDING" if ascending else "DESCENDING"
                }]
            }
        }]
    }
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def apply_multi_sort(service, spreadsheet_id: str, sheet_id: int, sort_specs: List[Dict],
                     start_row: int, end_row: int, num_cols: int):
    """Sort multiple columns"""
    specs = []
    for s in sort_specs:
        specs.append({
            "dimensionIndex": s["col_index"],
            "sortOrder": "ASCENDING" if s["ascending"] else "DESCENDING"
        })
    
    body = {
        "requests": [{
            "sortRange": {
                "range": {
                    "sheetId": sheet_id,
                    "startRowIndex": start_row,
                    "endRowIndex": end_row,
                    "startColumnIndex": 0,
                    "endColumnIndex": num_cols
                },
                "sortSpecs": specs
            }
        }]
    }
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def apply_filter(service, spreadsheet_id: str, sheet_id: int, col_index: int, operator: str, value):
    """Apply filter to column"""
    condition_map = {
        ">": "NUMBER_GREATER",
        "<": "NUMBER_LESS",
        "=": "NUMBER_EQ",
        "!=": "NUMBER_NOT_EQ"
    }
    
    body = {
        "requests": [{
            "setBasicFilter": {
                "filter": {
                    "range": {"sheetId": sheet_id},
                    "criteria": {
                        str(col_index): {
                            "condition": {
                                "type": condition_map.get(operator, "NUMBER_EQ"),
                                "values": [{"userEnteredValue": str(value)}]
                            }
                        }
                    }
                }
            }
        }]
    }
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def delete_rows_batch(service, spreadsheet_id: str, sheet_id: int, row_indices: List[int]):
    """Delete multiple rows (sorted in reverse to avoid index shifts)"""
    requests = []
    for idx in sorted(row_indices, reverse=True):
        requests.append({
            "deleteDimension": {
                "range": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "startIndex": idx,
                    "endIndex": idx + 1
                }
            }
        })
    
    body = {"requests": requests}
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def remove_duplicates(service, spreadsheet_id: str, sheet_id: int, col_index: int):
    """Remove duplicate rows based on column"""
    body = {
        "requests": [{
            "deleteDuplicates": {
                "range": {"sheetId": sheet_id},
                "comparisonColumns": [{"dimensionIndex": col_index}]
            }
        }]
    }
    return service.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()

def add_formula(service, spreadsheet_id: str, sheet_name: str, target_column: str, formula: str):
    """Add formula to target column"""
    body = {"values": [[formula]]}
    return service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_name}!{target_column}2",
        valueInputOption="USER_ENTERED",
        body=body
    ).execute()

# -------------------------
# MAIN AGENT CLASS
# -------------------------
class SheetsAIAgent:
    def __init__(self, credentials_path: str = "credentials.json", token_path: str = "token.json"):
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.service = None
        self.llm = setup_llm()
    
    def connect(self):
        """Connect to Google Sheets API"""
        self.service = authenticate_google(self.credentials_path, self.token_path)
    
    def execute(self, spreadsheet_id: str, sheet_name: str, user_prompt: str, header_row: int = 1):
        """
        Main execution method - handles all operations
        """
        if not self.service:
            self.connect()
        
        # Get headers
        header_a1 = f"{sheet_name}!{header_row}:{header_row}"
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
            return {"status": "error", "message": "Failed to parse instruction", "details": instruction}
        
        # Ground column names to actual headers
        instruction = ground_columns(instruction, headers)
        
        # Get all data for operations that need it
        all_vals = get_sheet_values(self.service, spreadsheet_id, f"{sheet_name}")
        num_rows = len(all_vals)
        num_cols = len(headers)
        
        action = instruction.get("action")
        result = {}
        
        # SORT
        if action == "sort":
            col_name = instruction["column"]
            col_idx = headers.index(col_name)
            ascending = instruction.get("ascending", True)
            
            result = apply_sort(
                self.service, spreadsheet_id, sheet_id, col_idx,
                header_row, num_rows, num_cols, ascending
            )
            return {
                "status": "success",
                "action": "sort",
                "column": col_name,
                "ascending": ascending,
                "response": result
            }
        
        # MULTI-COLUMN SORT
        elif action == "multicolumn_sort":
            sort_specs = []
            for s in instruction.get("sort", []):
                col_idx = headers.index(s["column"])
                sort_specs.append({
                    "col_index": col_idx,
                    "ascending": s.get("ascending", True)
                })
            
            result = apply_multi_sort(
                self.service, spreadsheet_id, sheet_id, sort_specs,
                header_row, num_rows, num_cols
            )
            return {
                "status": "success",
                "action": "multicolumn_sort",
                "columns": [s["column"] for s in instruction["sort"]],
                "response": result
            }
        
        # FILTER
        elif action == "filter":
            col_idx = headers.index(instruction["column"])
            result = apply_filter(
                self.service, spreadsheet_id, sheet_id,
                col_idx, instruction["operator"], instruction["value"]
            )
            return {
                "status": "success",
                "action": "filter",
                "column": instruction["column"],
                "operator": instruction["operator"],
                "value": instruction["value"],
                "response": result
            }
        
        # DELETE ROWS
        elif action == "delete_rows":
            col_name = instruction["column"]
            col_idx = headers.index(col_name)
            op = instruction["operator"]
            val = float(instruction["value"])
            
            delete_indices = []
            for i, row in enumerate(all_vals[1:], start=1):
                try:
                    cell_val = float(row[col_idx])
                    if (op == "<" and cell_val < val) or \
                       (op == ">" and cell_val > val) or \
                       (op == "=" and cell_val == val) or \
                       (op == "!=" and cell_val != val):
                        delete_indices.append(i)
                except:
                    pass
            
            if delete_indices:
                result = delete_rows_batch(self.service, spreadsheet_id, sheet_id, delete_indices)
            
            return {
                "status": "success",
                "action": "delete_rows",
                "column": col_name,
                "deleted_count": len(delete_indices),
                "response": result
            }
        
        # REMOVE DUPLICATES
        elif action == "remove_duplicates":
            col_idx = headers.index(instruction["column"])
            result = remove_duplicates(self.service, spreadsheet_id, sheet_id, col_idx)
            return {
                "status": "success",
                "action": "remove_duplicates",
                "column": instruction["column"],
                "response": result
            }
        
        # ADD FORMULA
        elif action == "formula":
            result = add_formula(
                self.service, spreadsheet_id, sheet_name,
                instruction["target_column"], instruction["formula"]
            )
            return {
                "status": "success",
                "action": "formula",
                "target_column": instruction["target_column"],
                "formula": instruction["formula"],
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
    parser.add_argument("--spreadsheet_id", required=True, help="Spreadsheet ID from URL")
    parser.add_argument("--sheet_name", default="Sheet1", help="Sheet name")
    parser.add_argument("--prompt", required=True, help="Natural language instruction")
    parser.add_argument("--credentials", default="credentials.json", help="OAuth credentials path")
    parser.add_argument("--token", default="token.json", help="Token path")
    
    args = parser.parse_args()
    
    agent = SheetsAIAgent(credentials_path=args.credentials, token_path=args.token)
    result = agent.execute(args.spreadsheet_id, args.sheet_name, args.prompt)
    
    print(json.dumps(result, indent=2))