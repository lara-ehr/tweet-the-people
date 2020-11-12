# Dockerfile for tweet_collect

# Use an official Python runtime as a parent image
FROM python:3.6-slim

# Name the working directory within the container to /app # /app exists only in the container
WORKDIR /app

# Copy the requirements file into the container at /app
# (a text files with all the libraries you want to install)
ADD credentials.py /app

ADD requirements.txt /app

ADD election_tweets.py /app

# Install any needed packages specified in requirements.txt
RUN pip install --trusted-host pypi.python.org -r requirements.txt

# Run election_tweets.py when the container launches
CMD ["python", "election_tweets.py"]
