FROM python:3.6

WORKDIR /usr/src/app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir --upgrade pip
RUN python -m pip install --user --upgrade git+https://github.com/twintproject/twint.git@origin/master#egg=twint
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "./streamer.py" ]