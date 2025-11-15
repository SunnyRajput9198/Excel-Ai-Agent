"use client";

import { useState } from "react";
import axios from "axios";

export default function Home() {
  const [sheetId, setSheetId] = useState("");
  const [sheetName, setSheetName] = useState("Sheet1");
  const [prompt, setPrompt] = useState("");
  const [excelFile, setExcelFile] = useState<File | null>(null);
  const [consoleOutput, setConsoleOutput] = useState("");

  const sendRequest = async () => {
    setConsoleOutput("Running agent...\n");

    const formData = new FormData();
    formData.append("prompt", prompt);
    formData.append("sheet_id", sheetId);
    formData.append("sheet_name", sheetName);

    if (excelFile) {
      formData.append("file", excelFile);
    }

    try {
      const res = await axios.post("http://127.0.0.1:8000/run-agent", formData, {
        headers: { "Content-Type": "multipart/form-data" },
      });

      setConsoleOutput(JSON.stringify(res.data, null, 2));
    } catch (error: any) {
      setConsoleOutput("Error:\n" + error.message);
    }
  };

  return (
    <div className="min-h-screen bg-gray-900 text-white p-8">
      <h1 className="text-3xl font-bold mb-6">AI Excel / Google Sheet Agent</h1>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
        
        {/* Input Panel */}
        <div className="bg-gray-800 p-6 rounded-xl shadow-xl space-y-4">
          
          <div>
            <label className="font-medium">Google Sheet ID</label>
            <input 
              className="w-full p-2 mt-1 bg-gray-700 rounded"
              value={sheetId}
              onChange={(e) => setSheetId(e.target.value)}
              placeholder="Optional if uploading Excel"
            />
          </div>

          <div>
            <label className="font-medium">Sheet Name</label>
            <input 
              className="w-full p-2 mt-1 bg-gray-700 rounded"
              value={sheetName}
              onChange={(e) => setSheetName(e.target.value)}
            />
          </div>

          <div>
            <label className="font-medium">Upload Excel File (.xlsx)</label>
            <input
              type="file"
              accept=".xlsx"
              className="mt-1"
              onChange={(e) => setExcelFile(e.target.files?.[0] || null)}
            />
          </div>

          <div>
            <label className="font-medium">AI Instruction</label>
            <textarea
              className="w-full p-2 mt-1 bg-gray-700 rounded h-32"
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="e.g., sort by CGPA highest first"
            />
          </div>

          <button
            onClick={sendRequest}
            className="w-full bg-blue-600 hover:bg-blue-700 p-3 rounded-lg mt-4"
          >
            Run Agent
          </button>
        </div>

        {/* Console Output */}
        <div className="bg-black p-6 rounded-xl shadow-xl">
          <h2 className="text-xl mb-2 font-bold">Console Output:</h2>
          <pre className="whitespace-pre-wrap bg-gray-900 p-4 rounded text-green-400">
            {consoleOutput}
          </pre>
        </div>
      </div>
    </div>
  );
}
