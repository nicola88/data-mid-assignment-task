FROM python:3.7

# Never prompts the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ENV AWS_ACCESS_KEY_ID="AKIAZZRUPVXQTWYNBBVC"
ENV AWS_SECRET_ACCESS_KEY="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"
ENV DB_HOST="localhost"
ENV DB_PORT=5432
ENV DB_USER="user"
ENV DB_PASSWORD="password"
ENV DB_TABLE="database"
ENV AWS_BUCKET_NAME="upday-data-assignment"
ENV AWS_FOLDER_NAME="lake/"

COPY . /app
WORKDIR /app

# Install python packages
RUN pip install --no-cache-dir -r /app/requirements.txt

CMD ["python", "./run.py"]
