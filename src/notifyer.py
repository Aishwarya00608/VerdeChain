def send_dynamic_alert(recipient_email, shipment_id, issue_type):
    msg = EmailMessage()
    msg.set_content(f"Hello, an issue of type '{issue_type}' was detected for shipment {shipment_id}.")
    msg['Subject'] = f'Verdechain Alert for {shipment_id}'
    msg['From'] = os.getenv("SMTP_USER")
    msg['To'] = recipient_email  # This is the dynamic part!

    # ... rest of the smtplib code remains the same ...