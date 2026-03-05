# 📊 Google Sheets AI Agent

An AI-powered assistant that performs **automatic operations on Google Sheets and Excel files** using natural language instructions.

This project uses:



- **FastAPI**  for backend API    
- **Google Sheets API v4**  
- **Gemini 2.0 Flash** LLM  
- **Python automation** for sheet operations (sorting, filtering, coloring, deleting rows, formulas)

---

## ✨ Features

### ✅ Natural Language → Google Sheets Actions  
Type anything like:

- “Sort by CGPA descending”
- “Delete rows where Package < 5”
- “Remove duplicates from Roll No”
- “Add formula in Total column”
- “Color rows where Category = General pink”
- “Color column C red”
- “Color range A2:C10 lightblue”

The agent converts your instruction into structured JSON using Gemini and performs the corresponding API operations.
   
---

## 🎯 Supported Operations

| Action | Description |
|-------|-------------|
| **Sort** | Sort a column ascending/descending |
| **Multi-column sort** | Sort by multiple columns |
| **Filter** | Filter rows based on condition |
| **Delete Rows** | Delete rows that match condition |
| **Remove Duplicates** | Remove duplicate entries |
| **Add Formula** | Insert formulas automatically |
| **Color Row** | Apply background color to a row |
| **Color Column** | Color full column |
| **Color Range** | Color any A1 range |
| **Color If** | Color rows based on value matching |

---

## 🧩 Excel File Support

You can upload an `.xlsx` file.  
Currently supported for:
- Local Excel sorting

More operations for Excel will be added later.

---

## 🚀 Tech Stack

- Python 3.10+
- FastAPI
- Google Sheets API
- Gemini 2.0 Flash (via LangChain)
- RapidFuzz (fuzzy column matching)
- Uvicorn

---

## 📁 Project Structure
- ├── ai_agent.py # Main AI logic
- ├── backend_api.py # FastAPI backend
- ├── credentials.json # Google OAuth client (user provides)
- ├── token.json # Auto-generated Google token
- ├── requirements.txt
- └── README.md



## 🧠 How It Works
- Reads Google Sheet headers

- Sends user instruction + headers to Gemini LL M

- LLM returns structured JSON describing the task

- Column names are corrected using fuzzy matching

- Performs the actual Google Sheet operation

- Returns response to frontend

## 🔮 Future Enhancements (Planned)
- Multi-sheet operations (students + company data)

- Pivot tables

- Insert rows/columns

- Auto formatting

- Full Excel file parity with Google Sheets actions


