@echo off
cd /d C:\capstone  
REM Run Python command and redirect stderr to NUL
python scripts/main_console.py

REM Display a message or pause at the end to indicate completion
echo Python script execution completed.
pause