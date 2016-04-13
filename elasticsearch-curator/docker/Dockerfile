FROM socrata/python

COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

COPY ship.d /etc/ship.d
