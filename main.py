from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from io import StringIO
import sys

# Pydantic model for incoming request data
class CodeExecution(BaseModel):
    code: str

app = FastAPI()

# --- The Code Execution Endpoint ---
@app.post("/run_script")
def run_script(execution: CodeExecution):
    # 1. Capture the code written by the user
    user_code = execution.code
    
    # 2. Setup to capture print output
    old_stdout = sys.stdout
    redirected_output = sys.stdout = StringIO()
    
    try:
        # 3. Execute the user's Python code
        # exec() is a dangerous function, but for a custom interpreter, it's necessary.
        exec(user_code)
        
        # 4. Get the captured output
        output = redirected_output.getvalue()
        
        # 5. Return success response with output
        return {"output": output, "error": None}
    
    except Exception as e:
        # 6. If an error occurs, capture the error message
        error_message = str(e)
        
        # 7. Return error response
        return {"output": None, "error": error_message}
        
    finally:
        # 8. Reset stdout to its original value
        sys.stdout = old_stdout

# Root endpoint (just for testing the server status)
@app.get("/")
def read_root():
    return {"status": "PyInterpreter API is running on Render!"}

