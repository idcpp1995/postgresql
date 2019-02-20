import argparse
import psycopg2
from cryptography.fernet import Fernet


def connect(src_user, src_database, src_password, src_host, src_port):
    global connection
    global cursor
    connection = psycopg2.connect(database=src_database, user=src_user,
                                  password=src_password, host=src_host, port=src_port)
    cursor = connection.cursor()
    cursor.execute("""SELECT  password FROM users""")

    results = cursor.fetchall()  # select cписка зашифрованного столбца из исходной таблицы (1)
    global bin_list
    bin_list = []  # представление (1) в бинарном виде, типа: b'...'

    for i in results:
        for j in i:
            a = str.encode(str(j))
            bin_list.append(a)


def decode(kkey, kkey2):
    dec_list = []  # список расшифрованных значений
    global new_code_list
    new_code_list = []  # список зашифрованных значений новым ключем
    for z in bin_list:
        f = Fernet(kkey)
        b = f.decrypt(z)
        n = b.decode('utf-8')  # перевод из b'...' в Юникод
        dec_list.append(n)
    for q in dec_list:
        ff = Fernet(kkey2)
        g = str.encode(str(q))  # перевод всех эл. списка в строку и перевод в b'...'
        token = ff.encrypt(g)
        my_decoded_str = token.decode()
        new_code_list.append(my_decoded_str)


def upload(dst_user, dst_database, dst_password, dst_host, dst_port):
    connection = psycopg2.connect(database=dst_database, user=dst_user,
                                  password=dst_password, host=dst_host, port=dst_port)
    k = [(st,) for st in new_code_list]
    cursor = connection.cursor()
    cursor.executemany("INSERT INTO users2 (password) VALUES (:1)", k)
    connection.commit()


parser = argparse.ArgumentParser(description='Enter dec_code and enc_code')

parser.add_argument('--src_user', '-src_user', nargs='?', const='airflow', type=str)
parser.add_argument('--src_database', '-src_database', nargs='?', const='airflow', type=str)
parser.add_argument('-src_password', type=str, help='password')
parser.add_argument('-src_host', type=str, help='host')
parser.add_argument('--src_port', '-src_port', nargs='?', const=5432, type=str)

parser.add_argument('kkey', type=str, help='Decode')
parser.add_argument('kkey2', type=str, help='Encode')

parser.add_argument('--dst_user', '-dst_user', nargs='?', const='airflow', type=str)
parser.add_argument('--dst_database', '-dst_database', nargs='?', const='airflow', type=str)
parser.add_argument('-dst_password', type=str, help='password')
parser.add_argument('-dst_host', type=str, help='host')
parser.add_argument('--dst_port', '-dst_port', nargs='?', const=5432, type=str)
args = parser.parse_args()

if __name__ == '__main__':
    connect(args.login, args.password, args.port_host)
    decode(args.kkey, args.kkey2)
    upload(args.up_login, args.up_password, args.up_port_host)