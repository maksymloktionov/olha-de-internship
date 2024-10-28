FROM python:3.9

# Install Java for Spark
RUN apt-get update && apt-get install -y openjdk-17-jdk \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

WORKDIR /usr/src/app


COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install pyspark 
RUN pip install pycountry
RUN pip install kagglehub

COPY . .

