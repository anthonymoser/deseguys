FROM python:3.10
WORKDIR /home/app

# Install requirements
COPY app/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the app
COPY app .
RUN wget https://storage.googleapis.com/pdt_central/deseguys/deseguys.duckdb
# Run app on port 8080
EXPOSE 8080
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8080"]