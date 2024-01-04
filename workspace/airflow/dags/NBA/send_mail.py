def run_send_mail():
    from NBA.config import Config
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    from email.mime.image import MIMEImage
    import os
    import urllib
    import requests
    from io import BytesIO
    import base64
    import json
    import smtplib
    import datetime
    now = datetime.datetime.now()
    add_time = datetime.timedelta(days=2)
    now = (now - add_time).strftime("%Y%m%d")

    with open(f'./NBA_data/data/{now}/{now}.txt', 'r',encoding="utf-8") as f:
        results = f.read()
    results = json.loads(results)
    html_table_tr='<table style="border: 1px solid black; border-collapse: collapse; margin-left: auto; margin-right: auto;"> <tr>'
    html_table_td='<tr>'
    for key,value in results.items():
        html_table_tr+='<th style="border: 1px solid black;border-collapse: collapse;">'+key+'</th>'
        html_table_td+='<td style="border: 1px solid black;border-collapse: collapse; text-align: center;">'+str(round(value,4))+'</td>'
    html_table_tr+='</tr>'
    html_table_td+='</tr> </table>'
    html_table=html_table_tr+html_table_td

    email = Config.to_email
    receiver = Config.from_email
    subject = Config.subject
    
    html_content_image = '<img src="https://lh3.googleusercontent.com/pw/ABLVV84lh78kBIeKLldjiO0dB0j5OjXtq_AjmHECKTC81AlrO1X-aq103FIERrW-9OSElGbRna1E2I3gZpOv7KmhDatPxr81jHdDLSR7L7UWzfpcPF83lUfCb_vzMDHAzn7QZzb6VOP6e9BbG78cVCz_ucY2=w819-h609-s-no-gm?authuser=0" alt="Logo" style="display: block; margin-left: auto; margin-right: auto; width: 50%;">'
    # show image in html
    # html_content = f"""
    # <html>
    #     <h1 style="text-align: center;">Ngày {now}</h1>
    #     <body>
    #         <p>
    #             {html_table}
    #         </p>
    #         {html_content_image}
    #     </body>
    #     <a class="p-follow__link" title="Follow on facebook" href="https://www.facebook.com/if.ky.tran"><span class="p-follow__logo p-follow__logo--facebook"><svg viewBox="0 0 512 512" class="icon"><path d="M388.4 85.3h-48c-37.6 0-45.1 17.9-45.1 44.2v58.1h89.9l-11.8 90.7H295v233.2h-93.9V278.7h-78.3V188h78.3v-67c0-77.7 47.4-119.9 116.8-119.9 33.2 0 61.8 2.6 70.2 3.5v80.9h.3z"></path></svg></span></a>
    #     <a class="p-follow__link" title="Follow on facebook" href="https://www.facebook.com/NDThanh2011"><span class="p-follow__logo p-follow__logo--facebook"><svg viewBox="0 0 512 512" class="icon"><path d="M388.4 85.3h-48c-37.6 0-45.1 17.9-45.1 44.2v58.1h89.9l-11.8 90.7H295v233.2h-93.9V278.7h-78.3V188h78.3v-67c0-77.7 47.4-119.9 116.8-119.9 33.2 0 61.8 2.6 70.2 3.5v80.9h.3z"></path></svg></span></a>
    # </html>
    # """
    # show image in html
# show image in html
# show image in html
    # show image in html
    html_content = f"""
    <html>
    <head>
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css" integrity="sha384-GLhlTQ8iRABdZLl6O3oVMWSktQOp6b7In1Zl3/Ji5PbN5Szkbe5P/SFIIJdA2gM6" crossorigin="anonymous">
    </head>
    <body>
    <h1 style="text-align: center;">Ngày {now}</h1>
    <p>
        {html_table}
    </p>
    {html_content_image}
    <div style="text-left: center; margin-top: 20px;">
        <a href="https://www.facebook.com/if.ky.tran" style="text-decoration: none; color: inherit;">
        <i class="fab fa-facebook-square" style="font-size: 48px; color: #3b5998;"></i>
        <p style="font-size: 14px; margin-top: 5px; color: #3b5998;">thanhND</p>
        </a>
    </div>
    <div style="text-left: center; margin-top: 20px;">
        <a href="https://www.facebook.com/NDThanh2011" style="text-decoration: none; color: inherit;">
        <i class="fab fa-facebook-square" style="font-size: 48px; color: #3b5998;"></i>
        <p style="font-size: 14px; margin-top: 5px; color: #3b5998;">moi</p>
        </a>
    </div>
    </body>
    </html>
    """


    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = email

    msg['To'] = ', '.join(receiver)
    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(html_table, 'html')
    part2 = MIMEText(html_content, 'html')
    msg.attach(part1)
    msg.attach(part2)
    
    smtpserver = smtplib.SMTP("smtp.gmail.com", 587)
    smtpserver.starttls()
    smtpserver.login(email, Config.key_email)
    smtpserver.sendmail(email, receiver, msg.as_string())
    smtpserver.quit()