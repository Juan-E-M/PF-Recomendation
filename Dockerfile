FROM python:3.9

# Set the working directory inside the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies your application needs
RUN pip install --no-cache-dir -r requirements.txt

# Expose the port on which your Flask app will run
EXPOSE 80

# Command to run your application
CMD ["python", "app.py"]