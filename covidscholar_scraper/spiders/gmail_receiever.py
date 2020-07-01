import datetime
import email
import imaplib

from pymongo import HASHED

from ._base import BaseSpider


class EmailSpider(BaseSpider):
    name = 'email'

    # DB specs
    collections_config = {
        'Scraper_covidscholar_receiever_gmail': [
            [('MessageId', HASHED)],
            'MessageId',
            'last_updated'
        ]
    }

    def start_requests(self):
        mail = imaplib.IMAP4_SSL('imap.gmail.com')
        mail.login(
            self.settings['COVIDSCHOLAR_RECEIVER_EMAIL'],
            self.settings['COVIDSCHOLAR_RECEIVER_PASSWORD'])
        mail.list()
        mail.select('inbox')

        _, message_ids = mail.uid('search', None, "ALL")
        message_ids = message_ids[0].decode('utf8').split()

        for message_uid in message_ids:
            if self.has_duplicate(
                    where='Scraper_covidscholar_receiever_gmail',
                    query={'MessageId': message_uid}):
                continue
            _, email_data = mail.uid('fetch', message_uid, '(RFC822)')
            raw_email = email_data[0][1].decode('utf-8')

            email_message = email.message_from_string(raw_email)

            date_tuple = email.utils.parsedate_tz(email_message['Date'])
            if date_tuple:
                time_received = datetime.datetime.fromtimestamp(email.utils.mktime_tz(date_tuple))
            else:
                time_received = None

            email_from = str(email.header.make_header(email.header.decode_header(email_message['From'])))
            email_to = str(email.header.make_header(email.header.decode_header(email_message['To'])))
            subject = str(email.header.make_header(email.header.decode_header(email_message['Subject'])))

            body = []
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        body.append(part.get_payload(decode=True).decode('utf8'))
                    except UnicodeEncodeError:
                        pass
                else:
                    continue

            item = {
                'MessageId': message_uid,
                'From': email_from,
                'To': email_to,
                'Subject': subject,
                'TimeReceived': time_received,
                'Body': body,
                'Raw': raw_email,
            }
            self.save_article(item, to='Scraper_covidscholar_receiever_gmail')

        yield from ()
