#!/bin/bash

# 1. Activate Python venv
source "D:/Excel-agent/venv/Scripts/activate"

echo "Python venv activated."

# 2. Start backend API
echo "Starting backend..."
python backend_api.py 

# 3. Start Node frontend
echo "Starting frontend..."
cd "D:/Excel-agent/excel-ai-ui"
npm run dev
