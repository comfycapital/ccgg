from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from datetime import datetime
import time
import random

# v.18

# Constants
DISCORD_LOGIN_URL = 'https://discord.com/login'
E_MAIL = 'your_email_here'
PASSWORD = 'your_password_here'
USERNAME_KEYWORD = 'comfy'  # Part of your username that is consistent
RECENT_MESSAGE_COUNT = 4
MESSAGE_ROW_SELECTOR = 'li[id^="chat-messages-"]'
MESSAGE_ARTICLE_SELECTOR = 'div[role="article"][data-list-item-id^="chat-messages___"]'
AUTHOR_ID_PREFIX = 'message-username-'
AUTHOR_NAME_SELECTOR = 'span[role="button"][data-text]'
ADD_REACTION_SELECTOR = '[role="button"][aria-label="Add Reaction"]'
EMOJI_SEARCH_SELECTOR = 'input[aria-controls="emoji-picker-grid"]'
REACTION_WAIT_TIMEOUT_SECONDS = 30

# Log file for links where recent message authors could not be fully checked
NO_USERNAME_LOG_FILE = 'no_username_channels.log'      # <<< NEW

MESSAGE_LINKS = [
    ('link', 'gm\n\nhave a great day!', [':star:', ':eyes:'], True),
    ('link', 'gn\n\nsleep well!', [':smiley:', ':relieved:', ':relaxed:'], False),
    # Add more links as needed, (channel link, message, emoji list, react flag)
]

def log_with_timestamp(message):
    """Print a message with the current timestamp."""
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(f"[{timestamp}] {message}")

def log_channel_no_usernames(channel_link):
    """Log the channel when recent message authors cannot be fully checked."""
    with open(NO_USERNAME_LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} - {channel_link}\n")

def random_sleep(min_seconds=1, max_seconds=2):
    """Sleep for a random amount of time between min_seconds and max_seconds."""
    time_to_sleep = random.uniform(min_seconds, max_seconds)
    time.sleep(time_to_sleep)

def login_to_discord(driver, email, password):
    driver.get(DISCORD_LOGIN_URL)
    random_sleep(7, 12)  # Wait after loading the page

    # Enter email using the name attribute
    email_input = driver.find_element(By.NAME, 'email')
    email_input.send_keys(email)
    log_with_timestamp("Entered email")
    random_sleep()

    # Enter password using the name attribute
    password_input = driver.find_element(By.NAME, 'password')
    password_input.send_keys(password)
    log_with_timestamp("Entered password")
    random_sleep()

    # Click login button
    login_button = driver.find_element(By.CSS_SELECTOR, 'button[type="submit"]')
    login_button.click()
    log_with_timestamp("Clicked login button")

    # Wait for login to complete
    random_sleep(7, 12)

def get_message_author(driver, message):
    """Resolve the referenced author header, including for grouped messages."""
    article = message.find_element(By.CSS_SELECTOR, MESSAGE_ARTICLE_SELECTOR)
    labelled_by = article.get_attribute('aria-labelledby') or ''
    author_ids = [
        element_id
        for element_id in labelled_by.split()
        if element_id.startswith(AUTHOR_ID_PREFIX)
    ]
    if len(author_ids) != 1:
        raise ValueError('Message article must reference exactly one author header.')

    author_header = driver.find_element(By.ID, author_ids[0])
    author_element = author_header.find_element(By.CSS_SELECTOR, AUTHOR_NAME_SELECTOR)
    username = author_element.get_attribute('data-text')
    if not username or not username.strip():
        raise ValueError('The referenced author header has no username in data-text.')
    return username


def recent_messages_thing(driver):
    """Return all selected recent authors, or an empty list if any cannot be resolved."""
    try:
        messages = driver.find_elements(By.CSS_SELECTOR, MESSAGE_ROW_SELECTOR)[-RECENT_MESSAGE_COUNT:]
        usernames = []
        for message in messages:
            username = get_message_author(driver, message)
            usernames.append(username)
            random_sleep()
        log_with_timestamp(f"Recent usernames: {usernames}")
        return usernames
    except Exception as e:
        log_with_timestamp(f"Could not fully resolve recent message authors: {e}")
        return []

def send_message(driver, link, message):
    try:
        driver.get(link)
        random_sleep(7, 12)

        # Check the 4 most recent messages and get the usernames
        recent_usernames = recent_messages_thing(driver)
        
        if not recent_usernames:
            log_with_timestamp("Recent message authors could not be fully checked. "
                               "Skipping sending the message.")
            log_channel_no_usernames(link)
            return False

        # Check if any username contains the keyword to avoid duplicate posting
        if any(USERNAME_KEYWORD in username for username in recent_usernames):
            log_with_timestamp(f"Message by '{USERNAME_KEYWORD}' found in the recent messages. Skipping this channel.")
            return False

        # Find the message input element and send the message
        message_input = driver.find_element(By.CSS_SELECTOR, 'div[role="textbox"]')
        for part in message.split('\n'):
            message_input.send_keys(part)
            ActionChains(driver).key_down(Keys.SHIFT).send_keys(Keys.ENTER).key_up(Keys.SHIFT).perform()
            random_sleep()

        message_input.send_keys(Keys.ENTER)
        log_with_timestamp(f"Sent message '{message}' to {link}")
        random_sleep()

        # Verify if the message was posted
        recent_usernames_after = recent_messages_thing(driver)
        if any(USERNAME_KEYWORD in username for username in recent_usernames_after):
            log_with_timestamp(f"Message successfully posted in {link}.")
            return True
        else:
            log_with_timestamp(f"Could not confirm the message was posted in {link}.")
            return False
    except Exception as e:
        log_with_timestamp(f"Failed to send message to {link}: {e}")
        return False

def find_and_react_to_most_recent_message(driver, username_keyword, emojis):
    try:
        messages = driver.find_elements(By.CSS_SELECTOR, MESSAGE_ROW_SELECTOR)
        for message in messages[::-1]:
            username = get_message_author(driver, message)
            if username_keyword not in username:
                continue

            log_with_timestamp(f"Found most recent message from user containing '{username_keyword}'")
            actions = ActionChains(driver)
            actions.move_to_element(message).perform()
            log_with_timestamp("Hovered over the message")
            random_sleep()

            react_button = WebDriverWait(message, REACTION_WAIT_TIMEOUT_SECONDS).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ADD_REACTION_SELECTOR)),
                message="Add Reaction did not become clickable within the selected message.",
            )
            react_button.click()
            log_with_timestamp("Clicked on add reaction")
            random_sleep()

            emoji_input = WebDriverWait(driver, REACTION_WAIT_TIMEOUT_SECONDS).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, EMOJI_SEARCH_SELECTOR)),
                message="The emoji search input did not become clickable after Add Reaction.",
            )
            emoji_input.click()
            log_with_timestamp("Clicked on emoji search input")
            random_sleep()

            emoji = random.choice(emojis)
            emoji_input.send_keys(emoji)
            random_sleep()
            emoji_input.send_keys(Keys.ENTER)
            log_with_timestamp(f"Submitted reaction with emoji '{emoji}'")
            return
        log_with_timestamp(f"No recent message found from user containing '{username_keyword}'")
    except Exception as e:
        log_with_timestamp(f"Failed to react to message: {e}")

def main():
    while True:
        # Initialize WebDriver in headless mode
        options = Options()
        options.add_argument('--headless')
        options.add_argument('--disable-gpu')
        options.add_argument('window-size=1920x1080')  # Optional but recommended
        driver = webdriver.Chrome(options=options)

        login_to_discord(driver, E_MAIL, PASSWORD)
        random_sleep(7, 12)  # Ensure login process is fully completed

        for link, message, emojis, react in MESSAGE_LINKS:
            try:
                message_sent = send_message(driver, link, message)
                if message_sent and react:
                    find_and_react_to_most_recent_message(driver, USERNAME_KEYWORD, emojis)
                    random_sleep(2, 4)  # Shorter random sleep after reacting
            except Exception as e:
                log_with_timestamp(f"Error occurred while processing link {link}: {e}")

        driver.quit()
        log_with_timestamp("Finished sending messages")

        # Sleep for 7 hours before running again
        log_with_timestamp("Sleeping for 7 hours...")
        time.sleep(7 * 3600)

if __name__ == "__main__":
    main()
