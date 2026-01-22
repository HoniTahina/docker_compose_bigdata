FROM jupyter/pyspark-notebook:latest

# Copier les notebooks et le producer
COPY notebooks/ /home/jovyan/work/notebooks/

# Installer kafka-python
RUN pip install kafka-python requests

# Changer le working dir
WORKDIR /home/jovyan/work



