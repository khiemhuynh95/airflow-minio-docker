FROM apache/airflow:2.7.3
USER root
# Install OpenJDK-11
RUN apt update && \
    apt-get install -y openjdk-11-jdk && \
    apt-get install -y ant && \
    apt-get clean;

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-amd64/
RUN export JAVA_HOME

# Install dependencies
RUN apt-get update && apt-get install -y wget ca-certificates
RUN apt-get install -y libpango-1.0-0 libxkbcommon0 xdg-utils fonts-liberation libgbm1

# Download and install Chrome
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN dpkg -i google-chrome-stable_current_amd64.deb 

# Set the default command
CMD ["google-chrome", "--version"]


USER airflow
# Install Python deps
COPY requirements.txt /requirements.txt
RUN pip install --user --upgrade pip
RUN pip install --no-cache-dir --user -r /requirements.txt