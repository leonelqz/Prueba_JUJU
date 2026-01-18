import smtplib, ssl
from email.message import EmailMessage


sender = "data_eng_team@juju.com"
receiver = "data_eng_team@juju.com"
apppass = "XXXXXXXXXXXX"  ## ESTO EN UN CASO REAL VIENE DE UN SECRET MANAGER!


def send_mail(body=None, email_sender=sender, email_receiver=receiver, gmail_apppass=apppass):
    msg = EmailMessage()
    msg["From"] = email_sender
    msg["To"] = email_receiver
    msg['Subject'] = "Alerta ETL prueba JUJU"

    msg.set_content(body)

    context = ssl.create_default_context()
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as smtp:
        smtp.login(email_sender, gmail_apppass)
        smtp.send_message(msg)
    print("Alerta enviada al correo")