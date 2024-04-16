import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

def send_emails(auth_message, recipient_email = ['Xunzhao.Yu@warwick.ac.uk']):
    # Set up the SMTP server
    smtp_server = 'smtp.gmail.com'
    smtp_port = 587  # This might vary based on your email provider
    sender_email = 'yuxunzhao@gmail.com'
    password = 'your password'
    #recipient_email = ['Xunzhao.Yu@warwick.ac.uk']  #, 'amrita.kulka@warwick.ac.uk', 'nikhil.datta@warwick.ac.uk']

    # Create message
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(recipient_email)
    message['Subject'] = 'LA Scraping Terminated'

    # Add body to email
    body = f"{auth_message} scraper is terminated."
    message.attach(MIMEText(body, 'plain'))

    # Connect to SMTP server
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()  # Secure the connection
    server.login(sender_email, password)

    # Send email
    text = message.as_string()
    server.sendmail(sender_email, recipient_email, text)

    # Close connection
    server.quit()
