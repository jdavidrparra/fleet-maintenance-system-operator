FROM python:3.11-slim
WORKDIR /app
COPY main.py .
RUN pip install kopf kubernetes
CMD ["kopf", "run", "main.py", "--standalone"]
