FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Load sample data and start the adapter
CMD ["sh", "-c", "python load_data.py && python adapter.py"]
