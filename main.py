#!/usr/bin/env python3
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
import ssl
import html2text

import random
import time
import datetime
import argparse
import json
import logging
from pathlib import Path
from contextlib import contextmanager
from typing import List, Optional, Dict, Any

from sqlalchemy import select, or_
from models import get_engine, Config, get_db, Session, init_db, session_scope, DatabaseError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# Constants
PAGE_LOAD_TIMEOUT = 5000  # milliseconds
MAX_RETRIES = 3
DEFAULT_CONFIG_PATH = './config/config.json'
MIN_WAIT_TIME = 45  # seconds
MAX_WAIT_TIME = 90  # seconds
SMTP_MAX_RETRIES = 3
SMTP_TIMEOUT = 30  # seconds
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Edge/115.0.1901.188',
]

HTML_EMAIL_TEMPLATE = """
<html>
    <head>
        <style>
            body { font-family: Arial, sans-serif; }
            .listing { margin-bottom: 20px; padding: 10px; border: 1px solid #ddd; border-radius: 5px; }
            a { color: #0066cc; text-decoration: none; }
            a:hover { text-decoration: underline; }
        </style>
    </head>
    <body>
        <h2>New Free Items Found on Craigslist</h2>
        {listings_html}
        <p style="color: #666; font-size: 12px;">
            This is an automated notification from your Craigslist scraper.
        </p>
    </body>
</html>
"""

def get_random_user_agent() -> str:
    """Get a random user agent from the list"""
    return random.choice(USER_AGENTS)

def random_sleep(min_seconds: float, max_seconds: float) -> None:
    """Sleep for a random amount of time with microsecond precision"""
    sleep_time = random.uniform(min_seconds, max_seconds)
    time.sleep(sleep_time)

@contextmanager
def get_browser():
    """Context manager for browser setup and cleanup with anti-bot detection measures"""
    playwright = None
    browser = None
    context = None
    
    try:
        playwright = sync_playwright().start()
        browser = playwright.chromium.launch(
            headless=True,
            args=[
                '--disable-gpu',
                '--no-sandbox',
                '--disable-dev-shm-usage',
                '--disable-blink-features=AutomationControlled',  # Hide automation
                '--disable-infobars',
                '--window-size=1920,1080',
                f'--user-agent={get_random_user_agent()}'
            ]
        )
        
        # Create a context with specific permissions and preferences
        context = browser.new_context(
            viewport={'width': 1920, 'height': 1080},
            user_agent=get_random_user_agent(),
            locale='en-US',
            timezone_id='America/New_York',
            permissions=['geolocation'],
            color_scheme='light',
            java_script_enabled=True,
            bypass_csp=True,  # Bypass Content Security Policy
        )

        # Add custom scripts to mask automation
        context.add_init_script("""
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            Object.defineProperty(navigator, 'plugins', {
                get: () => [
                    {
                        0: {type: "application/x-google-chrome-pdf"},
                        description: "Portable Document Format",
                        filename: "internal-pdf-viewer",
                        length: 1,
                        name: "Chrome PDF Plugin"
                    }
                ]
            });
        """)

        page = context.new_page()
        yield page
        
    except Exception as e:
        logger.error(f"Failed to initialize browser: {str(e)}")
        raise
    finally:
        try:
            if context: context.close()
            if browser: browser.close()
            if playwright: playwright.stop()
        except Exception as e:
            logger.error(f"Error during browser cleanup: {str(e)}")

def simulate_human_behavior(page):
    """Simulate human-like behavior on the page"""
    try:
        # Random mouse movements
        page.mouse.move(random.randint(100, 800), random.randint(100, 600))
        random_sleep(0.1, 0.3)
        
        # Random scroll behavior
        page.evaluate("""() => {
            window.scrollTo({
                top: Math.random() * document.body.scrollHeight * 0.8,
                behavior: 'smooth'
            });
        }""")
        random_sleep(0.5, 1.5)
        
        # Sometimes move mouse to a random link but don't click
        links = page.query_selector_all('a')
        if links and random.random() < 0.3:
            random_link = random.choice(links)
            random_link.hover()
            random_sleep(0.2, 0.5)
    except Exception as e:
        logger.error(f"Error during human behavior simulation: {str(e)}")

async def handle_dialog(dialog):
    """Handle any dialogs that might appear"""
    try:
        logger.info(f"Dialog appeared: {dialog.message}")
        await dialog.dismiss()
    except Exception as e:
        logger.error(f"Error handling dialog: {str(e)}")

def scrape_listings(page, timestamp: str, db) -> List:
    """Scrape listings from loaded page with anti-bot measures"""
    listings = []
    try:
        # Add random delay before starting
        random_sleep(1, 3)
        
        # Simulate human behavior
        simulate_human_behavior(page)
        
        # Wait for the listings container with random timeout
        timeout = random.randint(PAGE_LOAD_TIMEOUT - 1000, PAGE_LOAD_TIMEOUT + 1000)
        page.wait_for_selector('.cl-search-result', timeout=timeout)
        
        # Random delay before scraping
        random_sleep(0.5, 1.5)
        
        # Get all listings
        elements = page.query_selector_all('.cl-search-result')
        
        for el in elements:
            try:
                # Add slight random delay between processing elements
                random_sleep(0.1, 0.3)
                
                # Using Playwright's evaluation capabilities with added randomization
                listing_data = el.evaluate("""el => {
                    const randomDelay = (min, max) => 
                        new Promise(resolve => setTimeout(resolve, Math.random() * (max - min) + min));
                    
                    return new Promise(async (resolve) => {
                        await randomDelay(50, 150);
                        const title_el = el.querySelector('.posting-title');
                        await randomDelay(20, 100);
                        const meta_el = el.querySelector('.meta');
                        const [posted_time, location] = meta_el.textContent.split('·');
                        resolve({
                            title: title_el.textContent,
                            link: title_el.href,
                            posted_time: posted_time.trim(),
                            location: location.trim()
                        });
                    });
                }""")
                
                listings.append(db(
                    link=listing_data['link'],
                    title=listing_data['title'],
                    cl_id=listing_data['link'].split('/')[-1].removesuffix('.html'),
                    screenshot_path='',
                    time_posted=listing_data['posted_time'],
                    location=listing_data['location'],
                    time_scraped=timestamp
                ))
            except Exception as e:
                logger.error(f"Error parsing listing: {str(e)}")
                continue
                
    except PlaywrightTimeoutError:
        logger.info("Page load timeout - continuing with available content")
    except Exception as e:
        logger.error(f"Error scraping listings: {str(e)}")
        
    return listings

def load_config(config_path: str) -> Config:
    """Load and validate configuration file"""
    try:
        with open(config_path) as json_file:
            config_data = json.load(json_file)
            
        # Validate required fields
        required_fields = ['urls', 'email']
        missing_fields = [field for field in required_fields if field not in config_data]
        if missing_fields:
            raise ValueError(f"Missing required fields in config: {', '.join(missing_fields)}")
            
        # Validate email configuration if enabled
        if config_data.get('email', {}).get('enabled', False):
            email_fields = ['smtp_server', 'smtp_port', 'username', 'password', 'from_address', 'to_addresses']
            missing_email = [f for f in email_fields if f not in config_data['email']]
            if missing_email:
                raise ValueError(f"Missing email configuration fields: {', '.join(missing_email)}")
        
        return Config(**config_data)
    except json.JSONDecodeError as e:
        logger.error(f'Invalid JSON in config file: {str(e)}')
        raise
    except Exception as exc:
        logger.error(f'Configuration error: {exc}. Exiting...')
        raise

def format_listing_for_email(listing) -> str:
    """Format a single listing into an HTML section"""
    try:
        return f"""
        <div class="listing">
            <h3><a href="{listing.link}">{listing.title}</a></h3>
            <p>Location: {listing.location}</p>
            <p>Posted: {listing.time_posted}</p>
        </div>
        """
    except Exception as e:
        logger.error(f"Error formatting listing for email: {str(e)}")
        return ""

def send_email(subject: str, html_content: str, config: Config) -> None:
    """Send an HTML email with plain text fallback"""
    if not config.email.enabled:
        return

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = config.email.from_address
    msg['To'] = ', '.join(config.email.to_addresses)

    try:
        h = html2text.HTML2Text()
        h.ignore_links = False
        plain_text = h.handle(html_content)

        msg.attach(MIMEText(plain_text, 'plain'))
        msg.attach(MIMEText(html_content, 'html'))

        for attempt in range(SMTP_MAX_RETRIES):
            try:
                with smtplib.SMTP(config.email.smtp_server, config.email.smtp_port, timeout=SMTP_TIMEOUT) as server:
                    if config.email.smtp_use_tls:
                        server.starttls(context=ssl.create_default_context())
                    server.login(config.email.username, config.email.password)
                    server.send_message(msg)
                    logger.info(f"Email sent: {subject}")
                    break
            except (smtplib.SMTPServerDisconnected, smtplib.SMTPConnectError) as e:
                if attempt == SMTP_MAX_RETRIES - 1:  # Last attempt
                    raise
                logger.error(f"SMTP connection error (attempt {attempt + 1}/{SMTP_MAX_RETRIES}): {str(e)}")
                time.sleep(5)  # Wait before retry
    except Exception as e:
        logger.error(f"Failed to send email: {str(e)}")
        raise

def send_notification(listings: List, config: Config) -> None:
    """Send notification for new listings"""
    if not listings:
        return

    try:
        html_content = HTML_EMAIL_TEMPLATE.format(
            listings_html=''.join(format_listing_for_email(listing) for listing in listings)
        )
        subject = f"{config.email.notification_subject_prefix} {len(listings)} New Items Found"
        send_email(subject, html_content, config)
    except Exception as e:
        logger.error(f"Error sending notification: {str(e)}")

def send_error_notification(error_msg: str, config: Config) -> None:
    """Send error notification"""
    try:
        html_content = f"""
        <html><body>
            <h2>Craigslist Scraper Error</h2>
            <p style="color: red;">{error_msg}</p>
        </body></html>
        """
        subject = f"{config.email.notification_subject_prefix} Error Alert"
        send_email(subject, html_content, config)
    except Exception as e:
        logger.error(f"Failed to send error notification: {str(e)}")

def process_listings(listings: List, session, config: Config) -> None:
    """Process and store new listings"""
    try:
        new_listings = []
        for listing in listings:
            try:
                existing = session.query(db).filter_by(cl_id=listing.cl_id).first()
                if not existing:
                    session.add(listing)
                    new_listings.append(listing)
                    logger.info(f'New listing: {listing.title}')
                elif not existing.notified:
                    new_listings.append(existing)
                    existing.notified = True
            except Exception as e:
                logger.error(f"Error processing listing {listing.title}: {str(e)}")
                continue

        if new_listings:
            session.commit()
            try:
                if config.combine_notifications:
                    send_notification(new_listings, config)
                else:
                    for listing in new_listings:
                        send_notification([listing], config)
            except Exception as e:
                logger.error(f"Error sending notifications: {str(e)}")
                
    except Exception as e:
        logger.error(f"Error in process_listings: {str(e)}")
        session.rollback()
        raise

def main():
    parser = argparse.ArgumentParser(description='Craigslist Free Items Scraper')
    parser.add_argument('-c', '--config', default=DEFAULT_CONFIG_PATH,
                      help=f'Config file path. Default: {DEFAULT_CONFIG_PATH}')
    args = parser.parse_args()

    logger.info("Starting Craigslist Scraper")
    
    try:
        # Load and validate configuration
        config = load_config(args.config)
        validation_errors = config.validate()
        if validation_errors:
            for error in validation_errors:
                logger.error(f"Configuration error: {error}")
            return

        # Initialize database
        db_name = Path(args.config).stem
        db = get_db(db_name)
        engine = get_engine(
            user=config.db_user,
            password=config.db_password,
            host=config.db_host,
            port=config.db_port,
            database=config.db_name,
            echo=False,
            pool_size=config.db_pool_size,
            max_overflow=config.db_max_overflow,
            pool_timeout=config.db_pool_timeout
        )
        init_db(engine)
        
    except DatabaseError as e:
        logger.error(f"Database initialization failed: {str(e)}")
        return
    except Exception as e:
        logger.error(f"Failed to initialize: {str(e)}")
        return

    error_count = 0
    initial_run = True

    while True:
        try:
            timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            for i, url in enumerate(config.urls):
                for retry in range(MAX_RETRIES):
                    try:
                        with get_browser() as page:
                            # Configure page timeouts with randomization
                            random_timeout = random.randint(PAGE_LOAD_TIMEOUT - 1000, PAGE_LOAD_TIMEOUT + 1000)
                            page.set_default_timeout(random_timeout)
                            page.set_default_navigation_timeout(random_timeout)
                            
                            # Handle any dialogs that might appear
                            page.on('dialog', handle_dialog)
                            
                            # Navigate to the page with random wait until state
                            wait_until_states = ['networkidle', 'domcontentloaded', 'load']
                            response = page.goto(
                                url, 
                                wait_until=random.choice(wait_until_states),
                                timeout=random_timeout
                            )
                            
                            if not response or response.status >= 400:
                                raise Exception(f"Failed to load page: {response.status if response else 'No response'}")
                            
                            listings = scrape_listings(page, timestamp, db)
                            
                            if not initial_run:
                                with session_scope(engine) as session:
                                    process_listings(listings, session, config)
                            
                            # Random delay between URLs
                            if i < len(config.urls) - 1:
                                random_sleep(2, 5)
                                
                            break  # Success, exit retry loop
                            
                    except PlaywrightTimeoutError as e:
                        if retry == MAX_RETRIES - 1:  # Last attempt
                            raise
                        logger.error(f"Timeout on attempt {retry + 1}/{MAX_RETRIES}: {str(e)}")
                        random_sleep(5 * (retry + 1), 10 * (retry + 1))  # Exponential backoff
                        continue
                    except DatabaseError as e:
                        logger.error(f"Database error while processing listings: {str(e)}")
                        if retry == MAX_RETRIES - 1:
                            raise
                        random_sleep(5 * (retry + 1), 10 * (retry + 1))
                        continue
                        
            initial_run = False
            
            # Random sleep between cycles with some variation
            base_sleep = random.randint(MIN_WAIT_TIME, MAX_WAIT_TIME)
            jitter = random.uniform(-5, 5)  # Add/subtract up to 5 seconds
            sleep_seconds = max(MIN_WAIT_TIME, base_sleep + jitter)
            
            next_run = datetime.datetime.now() + datetime.timedelta(seconds=sleep_seconds)
            logger.info(f"Next run at: {next_run.strftime('%H:%M:%S')}")
            time.sleep(sleep_seconds)
            
        except KeyboardInterrupt:
            logger.info("Shutting down gracefully...")
            break
        except DatabaseError as e:
            logger.error(f"Critical database error: {str(e)}")
            random_sleep(30, 60)  # Longer delay after critical errors
        except Exception as e:
            logger.error(f"Critical error in main loop: {str(e)}")
            random_sleep(30, 60)  # Longer delay after critical errors

if __name__ == "__main__":
    main()
