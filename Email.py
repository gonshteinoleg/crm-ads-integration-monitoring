import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
from pretty_html_table import build_table

class Email:
    
    """ Класс для отправки письма на электронную почту.
    Получает на вход содержание письма.
    """
    
    def __init__(self, email_content):
        self.email_content = email_content

    def send_email(self):
        emails = ['gonshteinoleg@gmail.com']
        for element in emails:
            def py_mail(SUBJECT, BODY, TO, FROM):

                MESSAGE = MIMEMultipart('alternative')
                MESSAGE['subject'] = SUBJECT
                MESSAGE['To'] = TO
                MESSAGE['From'] = FROM

                HTML_BODY = MIMEText(BODY, 'html')

                MESSAGE.attach(HTML_BODY)

                server = smtplib.SMTP('smtp.gmail.com:587')

                server.set_debuglevel(1)

                password = "password"

                server.starttls()
                server.login(FROM, password)
                server.sendmail(FROM, [TO], MESSAGE.as_string())
                server.quit()

            email_content = self.email_content

            TO = element
            FROM = 'gonshteinoleg@gmail.com'

            py_mail("CRM & Ads Integration Alerting", email_content, TO, FROM)
