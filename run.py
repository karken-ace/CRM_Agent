#!/usr/bin/env python3
"""
Standalone runner for the agent.
Usage: python run.py
"""
import sys
from pathlib import Path

# Add the app directory to the path
sys.path.insert(0, str(Path(__file__).parent / 'app'))

if __name__ == "__main__":
    from app.main import app
    import uvicorn
    
    print("Starting agent on http://0.0.0.0:9000")
    print("Make sure the backend is running on http://localhost:8000")
    uvicorn.run(app, host="0.0.0.0", port=9000)

