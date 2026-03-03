FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir requests
COPY app.py .
COPY index.html .
EXPOSE 8080
CMD ["python", "app.py"]
