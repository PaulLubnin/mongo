import csv
from pymongo import MongoClient
from datetime import datetime


def read_data(csv_file, db):
    """
    Загрузить данные в бд из CSV-файла
    """
    with open(csv_file, encoding='utf8') as csvfile:
        # прочитать файл с данными и записать в коллекцию
        reader = csv.DictReader(csvfile)
        ticket_list = []
        for row in reader:
            row = dict(row)
            row['Цена'] = int(row['Цена'])
            row['Дата'] += '.2019'
            row['Дата'] = datetime.strptime(row['Дата'], '%d.%m.%Y')
            ticket_list.append(row)
            # print(row)
        db.insert_many(ticket_list)
        # print(db)

def find_cheapest(db):
    """
    Отсортировать билеты из базы по возрастания цены
    Документация: https://docs.mongodb.com/manual/reference/method/cursor.sort/
    """
    for item in list(db.find().sort('Цена')):
        print(f'{item["Цена"]} рублей за билет на концерт {item["Исполнитель"]}')

def find_by_name(name, db):
    """
    Найти билеты по имени исполнителя (в том числе – по подстроке),
    и вернуть их по возрастанию цены
    """
    for item in db.find({'Исполнитель': {'$regex': name}}, {'_id': 0}).sort('Цена'):
        print(f'{item["Исполнитель"]} - билет стоит {item["Цена"]}')

def find_by_date(start_date, end_date, db):
    """
    Найти билеты в заданном промежутке дат
    """
    start = datetime.strptime(start_date, '%d.%m.%Y')
    end = datetime.strptime(end_date, '%d.%m.%Y')
    for item in db.find({'Дата': {'$gte': start, '$lte': end}}, {'_id': 0}):
        print(f'{item["Дата"]} - выступает группа {item["Исполнитель"]}')


if __name__ == '__main__':
    client = MongoClient()
    concert_db = client['me']
    concert_collection = concert_db['concerts']
    read_data('artists.csv', concert_collection)
    print('\nСортировка по возрастанию цены:')
    find_cheapest(concert_collection)
    print('\nПоиск по имени:')
    find_by_name('Th', concert_collection)
    print('\nПоиск по дате:')
    find_by_date('01.07.2019', '30.07.2019', concert_collection)
    concert_collection.drop()
