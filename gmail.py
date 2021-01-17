# Python 3.8.0
import smtplib
import time
import sys
import sqlite3
import imaplib
import email
import traceback
import pickle
import argparse
from email.header import decode_header

# pip install alive-progress
from alive_progress import alive_bar


import sqlite3

FROM_EMAIL = "youremail@gmail.com"
FROM_PWD = "**********"
SMTP_SERVER = "imap.gmail.com"
SMTP_PORT = 993

use_unicode = True


def iterate_on_database(connection):
    try:
        cursor = connection.cursor()
        sql = """SELECT * from emails"""
        cursor.execute(sql)

        rowSize = 100
        while True:
            records = cursor.fetchmany(rowSize)
            print("got ", len(records), " rows")
            if not records:
                return
            for row in records:
                yield row

        cursor.close()
    except sqlite3.Error as error:
        print("Failed to read data from sqlite table", error)
    finally:
        if connection:
            connection.close()
            print("The SQLite connection is closed")


def read_all(connection):
    try:
        mail = imaplib.IMAP4_SSL(SMTP_SERVER)
        mail.login(FROM_EMAIL, FROM_PWD)
        mail.select("inbox")
        data = mail.search(None, "ALL")

        mail_ids = data[1]
        id_list = mail_ids[0].split()

        first_email_id = int(id_list[0])
        latest_email_id = int(id_list[-1])

        cursor = connection.cursor()

        print(first_email_id, latest_email_id)

        with alive_bar(latest_email_id) as bar:
            for i in range(latest_email_id, first_email_id, -1):
                status, data = mail.fetch(str(i), "(RFC822)")
                bar()
                if status != "OK":
                    print(status)
                    print(data)
                    print("===")
                    continue

                pickled_data = pickle.dumps(data)

                sql = f"INSERT INTO emails (data) VALUES (:data)"
                binary_data = sqlite3.Binary(pickled_data)
                cursor.execute(sql, {"data": binary_data})

                connection.commit()

    except Exception as e:
        traceback.print_exc()
        print(str(e))


def create_database(connection):
    cursor = connection.cursor()
    cursor.execute(
        """CREATE TABLE emails
                 (id INTEGER PRIMARY KEY, data blob, subject text, from_header text, to_header text, body text)"""
    )

    connection.commit()


def read_data(row):
    row_id = row[0]
    pickled_data = row[1]
    data = pickle.loads(pickled_data)
    return row_id, data


def parse(data):
    """
    return {subject: .., from_header, body...}
    """
    mail = {}
    for response_part in data:
        if isinstance(response_part, tuple):
            msg = email.message_from_bytes(response_part[1])
            mail["subject"] = msg["subject"]
            mail["from_header"] = msg["from"]
            mail["body"] = ""
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    mail["body"] += part.get_payload()
    return mail


def update_table(connection, parsed_data):
    cursor = connection.cursor()
    sql = f"UPDATE emails SET subject=:subject, from_header=:from_header, to_header=:to_header, body=:body WHERE id=:id"
    cursor.execute(sql, parsed_data)
    connection.commit()


connection = sqlite3.connect("gmail-emails.db")


parser = argparse.ArgumentParser(prog="gmail spam discovery application")

parser.add_argument(
    "--read_from_imap", default=False, action="store_true", help="reads into local db"
)

parser.add_argument(
    "--doparse", default=False, action="store_true", help="reads into local db"
)
args = parser.parse_args()

if args.read_from_imap:
    create_database(connection)
    read_all(connection)

if args.doparse:
    for row in iterate_on_database(connection):
        row_id, data = read_data(row)
        parsed_data = parse(data)
        parsed_data["id"] = row_id
        parsed_data["to_header"] = ""
        update_table(connection, parsed_data)
        # sys.exit(1)
