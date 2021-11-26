# Use the python slim base image
FROM python:3.7.7-slim

# Set workdirectory in container
WORKDIR /usr/src/prediction_service

# Copy the requirements.txt file to the container
COPY requirements.txt .

# Upgrade pip and install requirements
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --no-cache-dir -r requirements.txt

# Copy the source code to the container
COPY . .

# Expose container port 5000
EXPOSE 5000

# Run the app.py file
CMD ["python3","app.py"]