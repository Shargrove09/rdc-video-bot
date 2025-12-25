@echo off
cd /d "c:\repos\rdc-video-bot"
call .venv\Scripts\activate
cd rdc_video_bot
python script.py %*