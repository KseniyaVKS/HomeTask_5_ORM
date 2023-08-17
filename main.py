import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
from pprint import pprint

from models import create_table, Publisher, Book, Shop, Stock, Sale

DSN = 'postgresql://postgres:postgres@localhost:5432/bookstores'
engine = sqlalchemy.create_engine(DSN)
create_table(engine)

Session = sessionmaker(bind=engine)
session = Session()

# Функция чтения файла формата JSON
def read_json(file_path):
    with open(file_path, encoding="utf_8") as f:
        json_data = json.load(f)
    return json_data

# Функция заполнения таблиц данными
def add_data(data_list):
    for data in data_list:
        if data["model"] == "publisher":
            name = data["fields"]["name"]
            session.add(Publisher(name=name))
            session.commit()
        elif data["model"] == "book":
            title = data["fields"]["title"]
            id_publisher = data["fields"]["id_publisher"]
            session.add(Book(title=title, id_publisher=id_publisher))
            session.commit()
        elif data["model"] == "shop":
            name = data["fields"]["name"]
            session.add(Shop(name=name))
            session.commit()
        elif data["model"] == "stock":
            id_book = data["fields"]["id_book"]
            id_shop = data["fields"]["id_shop"]
            count = data["fields"]["count"]
            session.add(Stock(id_book=id_book, id_shop=id_shop, count=count))
            session.commit()
        elif data["model"] == "sale":
            price = data["fields"]["price"]
            date_sale = data["fields"]["date_sale"]
            id_stock = data["fields"]["id_stock"]
            count = data["fields"]["count"]
            session.add(Sale(price=price, date_sale=date_sale, id_stock=id_stock, count=count))
            session.commit()
        else: pass
    pass

# Функция, которая выводит покупки книг указанного автора
def get_purchases(author):
    if type(author) == str:
        purchases = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).\
            join(Shop).join(Sale).filter(Publisher.name.like(author))
    else:
        purchases = session.query(Book.title, Shop.name, Sale.price, Sale.date_sale).join(Publisher).join(Stock).\
            join(Shop).join(Sale).filter(Publisher.id == author)
    print(*([f'{s[0]} | {s[1]} | {str(s[2])} | {str(s[3])}' for s in purchases.all()]), sep='\n')


session.close()

if __name__ == '__main__':
    data_list = read_json('tests_data.json')
    add_data(data_list)
    get_purchases("O’Reilly")
    get_purchases(2)