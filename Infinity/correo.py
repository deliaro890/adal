import os
from dotenv import load_dotenv
import ssl 
from email.message import EmailMessage
import smtplib

#load_dotenv(os.path.join(project_folder, '.env'))


load_dotenv("variables.env")

def send_email (email_reciver : str , body : str ):
    """Send a email """

    email_sender = "tsarmatrixremaster@gmail.com"
    password = os.getenv("PASSWORD")
    #email_reciver = "adalbertomg07@gmail.com"
    subjet = "Correo de verificación"
    
    #body = "Tú código de verificación es : 123 "
    context = ssl.create_default_context()
    
    em = EmailMessage()
    em["From"] = email_sender
    em["To"] = email_reciver
    em["Subject"] = subjet
    em.set_content (body)
    
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context = context) as smtp :
        smtp.login (email_sender,password)
        smtp.sendmail(email_sender, email_reciver, em.as_string() )