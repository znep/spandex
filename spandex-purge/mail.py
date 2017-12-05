import sys
import smtplib
from email.mime.text import MIMEText


def email(mail_host, to_address, from_address, subject, text):
    msg = MIMEText(text, _subtype="html")
    msg["From"] = from_address
    msg["To"] = to_address
    msg["Subject"] = subject
    s = smtplib.SMTP(mail_host, port=25)
    s.send_message(msg)
    s.quit()

email(*sys.argv[1:6])
