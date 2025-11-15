from fastapi import FastAPI, UploadFile, Form
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import shutil
from ai_agent import SheetsAIAgent

app = FastAPI()

# CORS for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

agent = SheetsAIAgent()

@app.post("/run-agent")
async def run_agent(
    prompt: str = Form(...),
    sheet_id: str = Form(""),
    sheet_name: str = Form("Sheet1"),
    file: UploadFile | None = None,
):

    if file:
        path = "uploaded.xlsx"
        with open(path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        return {"status": "excel-uploaded", "file": path}

    # ❗FIX: Validate Google Sheet mode
    if not sheet_id or sheet_id.strip() == "":
        return {"error": "sheet_id_missing", "message": "You must pass a Google Sheet ID."}

    agent.connect()
    result = agent.execute(sheet_id, sheet_name, prompt)
    return result


if __name__ == "__main__":
    uvicorn.run("backend_api:app", host="127.0.0.1", port=8000, reload=True)
