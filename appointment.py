import smtplib
import requests
import asyncio
import json
import logging
import time
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from fake_useragent import UserAgent
from stem import Signal
from stem.control import Controller
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from concurrent.futures import ThreadPoolExecutor
from timeit import default_timer

##### SETTINGS START #####
BUERGERAMT_IDS = [ 122210, 122217, 122219, 122227, 122231, 122243, 122252, 122260, 122262, 122254, 122271, 122273, 122277, 122280, 122282, 122284, 122291, 122285, 122286, 122296, 327262, 325657, 150230, 122301, 122297, 122294, 122312, 122314, 122304, 122311, 122309, 317869, 324434, 122281, 324414, 122283, 122279, 122276, 122274, 122267, 122246, 122251, 122257, 122208, 122226 ]
SERVICE_IDS = [ 120686, 120335, 120697, 120703, 121151 ]
COMBINATIONS = [(x,y) for x in BUERGERAMT_IDS for y in SERVICE_IDS] 

MAIL_HOST = 'XXX'
MAIL_PORT = 587
MAIL_ADDRESS = 'XXX'
MAIL_PASSWORD = 'XXX'
MAIL_MESSAGE_FILE = 'message.txt'
##### SETTINGS END #####

START_TIME = default_timer()

class AppointmentDatesObject(object):
    date = ""
    url = ""

    def __init__(self, date, url):
        self.date = date
        self.url = url

class AppointmentsPerService(object):
    bid = 0
    sid = 0
    dates = []

    def __init__(self, bid, sid, dates):
        self.bid = bid
        self.sid = sid
        self.dates = dates

def get_tor_session():
    session = requests.session()
    session.proxies = {'http':  'socks5://127.0.0.1:9050',
                       'https': 'socks5://127.0.0.1:9050'}
    return session

def renew_tor_ip():
    with Controller.from_port(port=9051) as controller:
        controller.authenticate(password="XXX")
        controller.signal(Signal.NEWNYM)

def fetch(url):
    renew_tor_ip()
    appointments = []
    session = get_tor_session()
    headers = {'User-Agent': UserAgent().random}
    with session.get(url, headers=headers) as response:
        if response.status_code != 200:
            print("FAILURE::{0}::{1}".format(response.status_code, url))
            time.sleep(2)
            fetch(url)
        else:
            appointments = get_appointments_from_html(response.text, url)
            if(len(appointments) > 0):
                query = requests.utils.urlparse(url).query
                params = dict(x.split('=') for x in query.split('&'))
                bid = params['dienstleister']
                sid = params['anliegen[]']
                sendMail("marco@webexperte.berlin", bid, sid, appointments)
                log_appointment_dates(bid, sid, appointments)

    elapsed = default_timer() - START_TIME
    time_completed_at = "{:5.2f}s".format(elapsed)
    print("{0:<30} {1:>20}".format(url, time_completed_at))
    return appointments

def get_appointments_from_html(content, url):
    soup = BeautifulSoup(content, 'html.parser')
    month_widgets = soup.find_all(class_='calendar-month-table')
    today = datetime.now().date()
    available_dates = []
    for index, month_widget in enumerate(month_widgets):
        displayed_month = (today.month + index) % 12
        available_day_links = month_widget.find_all('td', class_='buchbar')
        for link in available_day_links:
            day = int(link.find('a').text)
            if day: 
                link = link.find('a').attrs['href']
                date = today.replace(month=displayed_month, day=day)
                if link:
                    dateObj = AppointmentDatesObject(date, url)
                    available_dates.append(dateObj)
    return available_dates

async def search():
    urls = []
    for combination in COMBINATIONS:
        buergeramt_id = combination[0]
        service_id = combination[1]
        url = "https://service.berlin.de/terminvereinbarung/termin/tag.php?termin=1&dienstleister=%s&anliegen[]=%s" % ( buergeramt_id, service_id)
        urls.append(url)
    print("{0:<30} {1:>20}".format("Combination", "Completed at"))
    with ThreadPoolExecutor(max_workers=3) as executor:
        loop = asyncio.get_event_loop()
        START_TIME = default_timer()
        tasks = [ loop.run_in_executor( executor, fetch, *(url,) ) for url in urls ]
        for response in await asyncio.gather(*tasks):
            pass

def log_appointment_dates(bid, sid, dates):
    """
    Writes the appointment dates in a file. Each line is written as a JSON object.
    """
    logging.basicConfig(filename='dates.log', format='%(message)s', level=logging.INFO)
    for date in dates:
        date_string = date.date.strftime('%d.%m.%Y')
        url = date.url
        logging.info(json.dumps({
        'timestamp': datetime.now().strftime('%Y-%m-%dT%H:%M:%S'),
        'bid': bid,
        'sid': sid,
        'date': date_string,
        'url': url
    }))

def read_template(filename):
    """
    Returns a Template object comprising the contents of the 
    file specified by filename.
    """
    with open(filename, 'r') as template_file:
        template_file_content = template_file.read()
    return Template(template_file_content)

def sendMail(to, bid, sid, dates):
    message_template = read_template(MAIL_MESSAGE_FILE)
    s = smtplib.SMTP(host=MAIL_HOST, port=MAIL_PORT)
    s.starttls()
    s.login(MAIL_ADDRESS, MAIL_PASSWORD)
    
    for date in dates:
        date_string = date.date.strftime('%d.%m.%Y')
        url = date.url
        msg = MIMEMultipart()
        message = message_template.substitute(BID=bid, SID=sid, DATE=date_string, URL=url)
        msg['From'] = MAIL_ADDRESS
        msg['To'] = to
        msg['Subject'] = "Freier Buergeramtstermin"
        msg.attach(MIMEText(message, 'plain'))
        s.send_message(msg)

    s.quit()

def observe(limit, polling_delay):
    """
    Polls for available appointments every [polling_delay] seconds for [limit] minutes/hours/days.
    :param limit: A timedelta. The observer will stop after this amount of time is elapsed
    :param polling_delay: The polling delay, in seconds.
    """
    START_TIME = default_timer()
    start = datetime.now()
    duration = timedelta()
    while duration < limit:
        duration = datetime.now() - start
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(search())
        loop.run_until_complete(future)
        time.sleep(polling_delay)

if __name__ == "__main__":
    logger = logging.getLogger('stem')
    logger.disabled = True
    observe(timedelta(days=365), polling_delay=30)

### ARCHIV

def go():
    logger = logging.getLogger('stem')
    logger.disabled = True
    observe(timedelta(days=365), polling_delay=30)

def search_appointment_dates():
    data = []
    for bid in BUERGERAMT_IDS:
        res = search_appointment_dates_for_buergeramt(bid)
        data.append(res)
    return data

def search_appointment_dates_for_buergeramt(bid):
    data = []
    for sid in SERVICE_IDS:
        appointment_dates = get_appointment_dates(bid, sid)
        if(len(appointment_dates) > 0):
            sendMail("marco@webexperte.berlin", bid, sid, appointment_dates)
            log_appointment_dates(bid, sid, appointment_dates)
        data.append(AppointmentsPerService(bid, sid, appointment_dates))
    return data

def get_appointment_dates(buergeramt_id, service_id):
    """
    Retrieves a list of appointment dates from the Berlin.de website.
    :param buergeramt_ids: A list of IDs of burgeramts to check
    :service_id: The service ID of the desired service. This is a URL parameter - the service ID has no meaning.
    :returns: A list of date objects
    """
    try:
        session = get_tor_session()
        url = "https://service.berlin.de/terminvereinbarung/termin/tag.php?termin=1&dienstleister=%s&anliegen[]=%s" % (
            buergeramt_id, service_id)
        headers = {'User-Agent': UserAgent().random}
        response = session.get(url, headers=headers)
        if response.status_code:
            soup = BeautifulSoup(response.text, 'html.parser')
            month_widgets = soup.find_all(class_='calendar-month-table')
            today = datetime.now().date()
            available_dates = []
            for index, month_widget in enumerate(month_widgets):
                displayed_month = (today.month + index) % 12
                available_day_links = month_widget.find_all('td', class_='buchbar')

                for link in available_day_links:
                    day = int(link.find('a').text)
                    if day: 
                        link = link.find('a').attrs['href']
                        date = today.replace(month=displayed_month, day=day)
                        if link:
                            # url = "https://service.berlin.de%s" % link
                            print (url)
                            dateObj = AppointmentDatesObject(date, url)
                            print(dateObj)
                            available_dates.append(dateObj)
    except Exception as exc:
         print(type(exc), str(exc))

    return available_dates

def get_appointment_times(time, session, headers):
    url = "https://service.berlin.de%s" % time
    print(url)
    if session.cookies:
        print (session.cookies.get_dict())
        cookies = session.cookies.get_dict()
        response = session.get(url, headers=headers, cookies=cookies)
    else:
        response = session.get(url, headers=headers)

    if session.cookies:
        print (session.cookies.get_dict())
    print(response.text)
    
    soup = BeautifulSoup(response.text, 'html.parser')
    timetable = soup.find(class_='timetable')
    available_times = []
    if timetable:
        available_time_row = timetable.find_all('th', class_='buchbar')
        print(available_time_row)
        available_times_row = timetable.find_all('td', class_='frei')
        print(available_times_row)
    return available_times
