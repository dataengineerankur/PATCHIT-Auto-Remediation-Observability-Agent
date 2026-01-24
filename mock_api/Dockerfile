FROM python:3.11-slim

WORKDIR /app
COPY pyproject.toml /app/pyproject.toml
COPY mock_api /app/mock_api

RUN pip install -U pip && pip install fastapi==0.115.6 uvicorn[standard]==0.34.0

EXPOSE 8099
CMD ["uvicorn", "mock_api.app:app", "--host", "0.0.0.0", "--port", "8099"]



