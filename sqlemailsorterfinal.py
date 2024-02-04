import mailbox
import re
from bs4 import BeautifulSoup
from langchain_openai import ChatOpenAI
from langchain_core.output_parsers import StrOutputParser
import psycopg2
from psycopg2 import sql

QUIT = "QUIT"
USER_ADDRESS = "aidanmtherrien@gmail.com"
API_KEY = ""

"""
Conversion from MBOX to Dictionary 
(None of these functions will be directly called.)
"""


def extract_body(message):
    """
    Parses email bodies
    :param message: full mbox set for a given email
    :return: An email body
    """
    # Check if the message is multipart
    if message.is_multipart():
        # If it is, iterate through the parts
        for part in message.walk():
            # Check if the part is text/plain
            if part.get_content_type() == "text/plain":
                # Return the content of the text/plain part
                try:
                    return part.get_payload(decode=True).decode(part.get_content_charset() or "utf-8", "ignore")
                except LookupError:
                    # Handle the case where the charset is not recognized
                    return part.get_payload(decode=True).decode("utf-8", "ignore")
    elif message.get_payload():
        # If the message is not multipart and has a payload, return the entire payload
        try:
            return message.get_payload(decode=True).decode(message.get_content_charset() or "utf-8", "ignore")
        except LookupError:
            # Handle the case where the charset is not recognized
            return message.get_payload(decode=True).decode("utf-8", "ignore")
    else:
        # If the body is None, return an empty string or handle it as needed
        return ""
    

def mbox_to_dict(mbox_name):
    """
    A Function to roughly convert your .mbox to a Dictionary
    :param mbox_name: MBOX file path
    :return: A list of dictionaries of emails
    """
    mbox = mailbox.mbox(mbox_name)

    emails = []
    for message in mbox:
        email_data = {
            "sender": message["From"],
            "recipient": message["To"],
            "subject": message["Subject"],
            "gmail_id": message["Message-ID"],
            "replied": message["In-Reply-To"],
            "email_date": message["Date"],
            "perm_type": "",
            "body": extract_body(message)
        }
        emails.append(email_data)

    return emails


def strip_html(html):
    """
    A function to remove HTML tags from a string
    :param html: Passed string
    :return: Cleaned String
    """
    if html is None:
        return ""

    # Use BeautifulSoup to parse the HTML and get the text without tags
    soup = BeautifulSoup(html, 'html.parser')
    text = soup.get_text(separator=' ', strip=True)

    # Remove escape sequences and extra whitespaces
    cleaned_text = re.sub(r'[\n\'\u200c\s]+', ' ', text).strip()

    return cleaned_text


def remove_links(text):
    """
    Function to remove any links from a passed string.
    :param text: Passed string.
    :return: Cleaned String
    """
    def replace_url(match):
        return ''

    # Regular expression to match URLs
    url_pattern = re.compile(r'https?://\S+|www\.\S+|ftp://\S+|\b(?:[a-z0-9](?:[-.a-z0-9]*[a-z0-9])?\.[a-z](?:[-.a-z0-9]*[a-z0-9])+(?::\d+)?\b)')

    # Remove URLs from the text
    cleaned_text = url_pattern.sub(replace_url, text)

    return cleaned_text


def strip_repeated_characters(text):
    """
    Function to get rid of visual blocks in emails.
    :param text: Passed String
    :return: Cleaned String
    """
    # Regular expression to match consecutive repeated non-alphanumeric characters
    repeated_chars_pattern = re.compile(r'([^a-zA-Z0-9])\1+')

    # Remove consecutive repeated characters from the text
    cleaned_text = repeated_chars_pattern.sub(r'\1', text)

    return cleaned_text


def html_loop(all_emails):
    """
    Loops through all emails and formats them all using helper functions.
    :param all_emails: List of dictionaries, each dictionary being an email.
    :return: Formatted emails in a list of dictionaries.
    """
    for i in range(len(all_emails)):
        # Strip HTML
        all_emails[i]["body"] = strip_html(all_emails[i]["body"])
        # Remove Links
        all_emails[i]["body"] = remove_links(all_emails[i]["body"])
        # Strip Formatting Characters
        all_emails[i]["body"] = strip_repeated_characters(all_emails[i]["body"])
        # Format Addresses
        all_emails[i]["sender"] = format_addresses(all_emails[i]["sender"])

    return all_emails


def format_addresses(addy):
    """
    Isolates the actual email address
    :param addy:
    :return: Formatted Address
    """
    for i in range(len(addy)):
        if addy[i] == "<":
            address = addy[i + 1:-1]
            return address
    return addy


"""
Known Email Functions
"""

def fetch_column_data(connection, table_name, column_name):
    try:
        with connection.cursor() as cursor:
            select_query = f"SELECT {column_name} FROM {table_name};"
            cursor.execute(select_query)
            column_data = cursor.fetchall()

            # Extracting data from the result and putting it in a list
            data_list = [row[0] for row in column_data if row[0] is not None]

            return data_list

    except psycopg2.Error as e:
        print(f"Error: Unable to fetch data from {column_name} column in {table_name} table")
        print(e)


def find_replied_email_addresses(emails):
    replied_email_addresses = set()

    # Create a dictionary to map Message-IDs to email addresses
    message_id_to_from_address = {email['gmail_id']: email['sender'] for email in emails}

    # Iterate through each email
    for email in emails:
        # Check if the current email has an "In-Reply-To" field
        if 'gmail_id' in email:
            # Extract the Message-ID from the "In-Reply-To" field
            in_reply_to_message_id = email['gmail_id']

            # Check if the Message-ID is in the dictionary
            if in_reply_to_message_id in message_id_to_from_address:
                # If yes, add the original email's From address to the replied_email_addresses set
                replied_from_address = message_id_to_from_address[in_reply_to_message_id]
                replied_email_addresses.add(replied_from_address)

    # Cleaning Email Addresses
    replied_email_addresses = list(replied_email_addresses)
    for i in range(len(replied_email_addresses)):
        replied_email_addresses[i] = format_addresses(replied_email_addresses[i])

    return replied_email_addresses



"""
Database Background
"""


def load_defaults(connection):
    # Starter for LLM to have context
    default_wants = [
        'New Project',
        'Question about billing',
        'When will a deliverable be delivered',
        'Problem with the work',
        'Terminate Contract',
        'Renew Contract',
        'Expand project',
        'Change scope',
        'ask for an estimate',
        'payment questions',
        'new work',
        'agreement in place'
    ]

    default_sender_types = [
        'Client',
        'Vendor',
        'Marketing',
        'Junk',
        'Employees',
        'Prospective Client',
        'Contractor',
    ]

    default_alerts = [
        'Client Communication',
        'Deliverable not working',
        'Billing problem'
    ]

    # Loading into SQL
    write_list_to_sql_column(connection, f"{username}extras", "want_types", default_wants)
    write_list_to_sql_column(connection, f"{username}extras", "sender_types", default_sender_types)
    write_list_to_sql_column(connection, f"{username}extras", "alerts_types", default_alerts)
    pass


def insert_data(cursor, table_name, data):
    # Insert data into the existing table, excluding the key "body"
    for entry in data:
        columns = [col for col in entry.keys() if col != "body"]
        # Truncate or handle long strings
        values = [
                str(entry[column])[:255] if (column in entry and entry[column] is not None) else None
                for column in columns
        ]


        insert_query = sql.SQL("INSERT INTO {} ({}) VALUES ({});").format(
            sql.Identifier(table_name),
            sql.SQL(', ').join(map(sql.Identifier, columns)),
            sql.SQL(', ').join([sql.Placeholder()] * len(columns))
        )

        cursor.execute(insert_query, values)


def permanent_tags(connection, email):
    """
    If 3 emails in a row have the same sender type, call that the permanent tag.
    """
    emails = search_recent_entries(connection, email['sender'])

    # Existing Perm Tag
    if email['perm_type'] != "":
        return email['perm_type']
    # Enough Data
    elif len(emails) == 3:
        matches = 0
        prev = ""
        for i in range(len(emails)):
            if i == 0:
                prev = emails[i]['sender_type']
            else:
                if emails[i]['sender_type'] == prev:
                    matches += 1
        if matches == 2:
            return prev
        else:
            return ""
    # Not Enough Data
    else:
        return ""




"""
LLM Calling
"""


def prompt_updater(email, connection):
    # Getting Necessary Data
    known = fetch_column_data(connection, f"{username}extras", "known_addresses")
    alert_types = fetch_column_data(connection, f"{username}extras", "alerts_types")
    sender_types = fetch_column_data(connection, f"{username}extras", "alerts_types")
    want_types = fetch_column_data(connection, f"{username}extras", "want_types")


    # Known Sender Info
    if email['sender'] in known:
        send_string = "True"
    else:
        send_string = "False"


    # Building Prompt
    prompt = f"""You will be passed the following information surrounding an email:
            Sender, Subject, Date, Body, and whether the sender is known to the user.
            Sender = {email['sender']}, 
            Subject = {email['subject']}, 
            Date = {email['email_date']}, 
            Body = {email['body']}, 
            Known = {send_string},
            With the given information, act as an AI email assistant for a software engineer working at Atigro
            by marking emails as follows: 
            1) Determine what type of sender sent the email, choose from the following if applicable({', '.join(sender_types)}) If the email cannot be categorized into those, create a new sender type that describes the sender DO NOT SAY UNKNOWN.
            2) Determine what the sender wants, choose from the following if applicable ({', '.join(want_types)}) If the email cannot be categorized into those, create a new want that describes what the sender wants.
            3) Determine what alert the email falls under, choose from the following if applicable ({', '.join(alert_types)}) If the email cannot be categorized into those, create a new alert that describes the email in 5 words or less.
            4) Give each email an score out of 100 determining how urgently the software engineer needs to respond to the email. Give marketing emails a score of 0.
            Your response should be formatted as follows. Do not label the data, just print it. '|||' should be included.
            [Sender Type] ||| [Sender Want] ||| [Alert] ||| [Urgency(just the number)]
            """
            
    return prompt


def lang_call(email, connection):
    """
    Calls ChatGPT using langchain and returns the response.
    :param email: Dictionary containing email.
    :return: ChatGPT Response
    """

    # Langchain Call
    llm = ChatOpenAI(openai_api_key=API_KEY)
    output_parser = StrOutputParser()
    chain = llm | output_parser
    output = chain.invoke(prompt_updater(email, connection))

    # Append Dictionaries
    output = output.replace("[", "").replace("]", "")
    output = output.split("|||")
    email["sender_type"] = output[0].strip()
    email["sender_want"] = output[1].strip()
    email["alert"] = output[2].strip()
    email["urgency"] = int(output[3].strip())

    return email


"""
Frequently Used Functions
"""
 

def write_to_extras(emails, connection, known):
    """
    Function to write necessary data to extras table
    """

    # Pulling from List of Dictionaries
    alerts_types = []
    sender_types = []
    sender_wants = []
    for i in range(len(emails)):
        alerts_types.append(emails[i]["alert"])
        sender_types.append(emails[i]["sender_type"])
        sender_wants.append(emails[i]["sender_want"])

    # Writing to SQL
    write_list_to_sql_column(connection, f"{username}extras", "alerts_types", alerts_types)
    write_list_to_sql_column(connection, f"{username}extras", "sender_types", sender_types)
    write_list_to_sql_column(connection, f"{username}extras", "want_types", sender_wants)
    write_list_to_sql_column(connection, f"{username}extras", "known_addresses", known)



def mbox_format(mbox):
    """
    Converts mbox to dict and cleans data.
    """
    l_o_d = mbox_to_dict(mbox)
    l_o_d = html_loop(l_o_d)
    return l_o_d


def email_operations(emails, connection, count):
    """
    Iterates through list of dictionaries and gets gpt response for all.
    :param emails: List of dictionaries containing emails.
    :param known: List of known email addresses
    :return: List of dictionaries with response keys
    """
    # Determine Range of Loop
    if count.lower() == "all":
        count = len(emails)
    elif count.isnumeric():
        count = int(count)

    responses = []
    for i in range(count):
        responses.append(lang_call(emails[i], connection))
        emails[i]["perm_type"] = permanent_tags(connection, emails[i])
    return responses


def bubble_sort(dictionary_list):
    """
    Bubble sorts emails by urgency
    :param dictionary_list: Unsorted list
    :return: Sorted list
    """
    n = len(dictionary_list)

    for i in range(n - 1):
        for j in range(0, n - i - 1):
            # Compare scores and swap if needed
            if dictionary_list[j]["urgency"] < dictionary_list[j + 1]["urgency"]:
                dictionary_list[j], dictionary_list[j + 1] = dictionary_list[j + 1], dictionary_list[j]

    return dictionary_list


"""
SQL Functions
"""


def search_recent_entries(connection, sender_column_value):
    # Assuming the table name is 'your_table_name' and column name is 'sender'
    table_name = f'{username}emails'
    sender_column_name = 'sender'
    
    # Create a cursor object
    cursor = connection.cursor()

    try:
        # Query to select the 3 most recent entries with matching sender value
        query = f"SELECT * FROM {table_name} WHERE {sender_column_name} = %s ORDER BY id DESC LIMIT 3"
        
        # Execute the query
        cursor.execute(query, (sender_column_value,))
        
        # Fetch all the rows
        rows = cursor.fetchall()

        if not rows:
            # If no entries found, return an empty list
            return []

        # Extracting column names from the cursor description
        column_names = [desc[0] for desc in cursor.description]

        # Assemble the result into a list of dictionaries
        result_list = []
        for row in rows:
            entry_dict = dict(zip(column_names, row))
            result_list.append(entry_dict)

        return result_list

    except Exception as e:
        print(f"Error: {e}")

    finally:
        # Close the cursor
        cursor.close()



def delete_table(conn, table_name):
    try:
        # Create a cursor object
        with conn.cursor() as cursor:
            # Use sql.SQL and sql.Identifier to safely compose the SQL statement
            table_identifier = sql.Identifier(table_name)
            query = sql.SQL("DROP TABLE IF EXISTS {}").format(table_identifier)
            
            # Execute the query
            cursor.execute(query)
            
            # Commit the changes
            conn.commit()
            
            print(f"Table '{table_name}' deleted successfully.")
    
    except psycopg2.Error as e:
        print(f"Error: {e}")
        conn.rollback()


def connect_to_db():
    """
    Initial Connection to Database
    """
    try:
        connection = psycopg2.connect(
            user="sqladmin",
            password="1234",
            host="localhost",
            port="5432",
            database="aiprogram"
        )
        print("Successfully Connected to SQL")
        return connection
    except psycopg2.Error as e:
        print("Error: Unable to connect to the database")
        print(e)
        return None
    

def create_emails_table(connection):
    try:
        # Define the SQL statement to create the 'emails' table
        create_table_query = sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {username}emails (
                id SERIAL PRIMARY KEY,
                sender VARCHAR(255),
                recipient VARCHAR(255),
                subject VARCHAR(255),
                gmail_id VARCHAR(225),                     
                replied VARCHAR(225),
                email_date VARCHAR(225),
                perm_type VARCHAR(255),
                urgency VARCHAR(225),
                known VARCHAR(225),
                alert VARCHAR(225),
                sender_type VARCHAR(225),
                sender_want VARCHAR(225)
            );
        """)

        # Establish a connection and create a cursor
        with connection, connection.cursor() as cursor:
            # Check if the table exists before attempting to create it
            cursor.execute(f"SELECT to_regclass('{username}emails')")
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                # Execute the SQL statement to create the table
                cursor.execute(create_table_query)
                print(f"Table '{username}emails' created successfully!")
            else:
                print("Table 'emails' already exists.")

    except Exception as e:
        # Handle any exceptions that might occur during table creation
        print(f"Error creating table: {e}")


def create_extras_table(connection):
    try:
        # Define the SQL statement to create the 'emails' table
        create_table_query = sql.SQL(f"""
            CREATE TABLE IF NOT EXISTS {username}extras (
                id SERIAL PRIMARY KEY,
                alerts_types VARCHAR(255),
                sender_types VARCHAR(255),
                known_addresses VARCHAR(225),
                want_types VARCHAR(225)                     
            );
        """)

        # Establish a connection and create a cursor
        with connection, connection.cursor() as cursor:
            # Check if the table exists before attempting to create it
            cursor.execute(f"SELECT to_regclass('{username}extras')")
            table_exists = cursor.fetchone()[0]

            if not table_exists:
                # Execute the SQL statement to create the table
                cursor.execute(create_table_query)
                print(f"Table '{username}extras' created successfully!")
            else:
                print(f"Table '{username}extras' already exists.")

    except Exception as e:
        # Handle any exceptions that might occur during table creation
        print(f"Error creating table: {e}")


def show_all_emails(connection):
    """
    Prints Entire Table
    """
    if connection:
        try:
            with connection.cursor() as cursor:
                select_all_query = sql.SQL(f"SELECT * FROM {username}emails;")
                cursor.execute(select_all_query)
                emails = cursor.fetchall()

                if not emails:
                    print("No emails found.")
                else:
                    for email in emails:
                        print(email)
        except psycopg2.Error as e:
            print("Error: Unable to fetch emails")
            print(e)


def write_list_to_sql_column(connection, table_name, column_name, values_list):
    try:
        # Create a cursor object to interact with the database
        with connection.cursor() as cursor:
            # Ensure the column and table names are safe to use in SQL queries
            safe_column_name = sql.Identifier(column_name)
            safe_table_name = sql.Identifier(table_name)

            # Select existing values in the column
            select_query = sql.SQL("SELECT {} FROM {}").format(safe_column_name, safe_table_name)
            cursor.execute(select_query)

            # Fetch all existing values from the column
            existing_values = set(row[0] for row in cursor.fetchall())

            # Filter out values that already exist in the column
            new_values = [value for value in values_list if value not in existing_values]

            if new_values:
                # Construct the SQL query to insert new values
                insert_query = sql.SQL("INSERT INTO {} ({}) VALUES {}").format(
                    safe_table_name,
                    safe_column_name,
                    sql.SQL(', ').join(sql.Placeholder() * len(new_values))
                )

                # Execute the query with the new values
                cursor.execute(insert_query, [(value,) for value in new_values])

                # Commit the changes to the database
                connection.commit()

                print(f"Values successfully inserted into {column_name} column.")
            else:
                print("No new values to insert.")

    except psycopg2.Error as e:
        # Handle any errors that may occur during the process
        print(f"Error: {e}")
        connection.rollback()

    finally:
        # Close the cursor and release resources
        cursor.close()


def transfer_data_to_sql(data, table_name, connection):
    try:
        
        # Create a cursor
        cursor = connection.cursor()

        # Insert data into the existing table (excluding "body")
        insert_data(cursor, table_name, data)

        # Commit the changes
        connection.commit()

    except psycopg2.Error as e:
        print("Error:", e)

    finally:
        # Close the cursor (Note: Connection is not closed to allow reuse)
        if cursor:
            cursor.close()



"""
Menu Items
"""


def new_inbox(database):
    # Getting Necessary Input
    mbox = input("Enter your MBOX file path: ")
    count = input("How many emails would you like to process? ('all' for whole inbox): ")
    # Deletes Old Table if it Exists and Creates New
    delete_table(database, f"{username}emails")
    delete_table(database, f"{username}extras")
    create_emails_table(database)
    create_extras_table(database)
    # Loading Defaults
    load_defaults(database)
    # Get Data From MBOX
    mbox_f = mbox_format(mbox)
    # Automatically Checks for Known Emails
    known = find_replied_email_addresses(mbox_f)
    # LLM Calling
    operated = email_operations(mbox_f, database, count)
    # Writes Data to SQL
    transfer_data_to_sql(operated, f"{username}emails", database)
    write_to_extras(operated, connection, known)
    print("Create New Inbox Run")
    pass


if __name__ == "__main__":
    connection = connect_to_db()
    global username
    username = input("Enter your username: ")
    user_input = ""
    past_commands = []
    while user_input != QUIT:
        user_input = input("Enter Command: ").upper()
        past_commands.append(user_input)
        if user_input == "NEW_INBOX":
            new_inbox(connection)
        elif user_input == "SHOW_EMAILS":
            show_all_emails(connection)

