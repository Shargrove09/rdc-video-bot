@echo off
cd /d "c:\repos\rdc-video-bot"
echo Starting RDC Video Bot at %date% %time% >> task_log.txt
call .venv\Scripts\activate >> task_log.txt 2>&1
cd rdc_video_bot
python script.py %* >> ..\task_log.txt 2>&1
echo Finished at %date% %time% >> ..\task_log.txt
echo ================================ >> ..\task_log.txt