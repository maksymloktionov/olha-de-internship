FROM python:3.12.3

WORKDIR /usr/src/app


# Copy the Python script into the container
COPY python.py .

CMD [ "python", "./python.py" ]
