FROM python:3.6

# Create app directory
WORKDIR /usr/src/app

# Install app dependencies
# A wildcard is used to ensure both package.json AND package-lock.json are copied
# where available (npm@5+)
COPY requirements.txt ./

RUN pip install -r requirements.txt

# Bundle app source
COPY . .

CMD [ "python", "app.py" ]
