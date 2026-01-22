#!/bin/bash
# Lancer le producer en arrière-plan
python /home/jovyan/work/notebooks/weather_producer.py &

# Lancer Jupyter Notebook
start-notebook.sh
