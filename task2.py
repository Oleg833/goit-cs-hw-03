from connect_db import create_connect
from pymongo import errors
from bson.objectid import ObjectId

client = create_connect()
db = client["db-cats"]
cats_collection = db["cats"]


def get_all(name: str | None = None) -> list[dict]:
    try:
        if name:
            return list(cats_collection.find({"name": {"$regex": name}}))
        return list(cats_collection.find())
    except errors.PyMongoError as e:
        print(f"Помилка при отриманні всіх котів: {e}")
        return []


def get_by_id(id: ObjectId) -> dict | None:
    try:
        return cats_collection.find_one({"_id": id})
    except errors.PyMongoError as e:
        print(f"Помилка при отриманні кота за ID {id}: {e}")
        return None


def create():
    try:
        name = input("Введіть імʼя кота для створення: ")
        age = int(input("Введіть вік кота: "))
        features_input = input("Введіть особливості кота, розділені комою: ")
        features = features_input.split(",")
        data = {"name": name, "age": age, "features": features}

        result_one = cats_collection.insert_one(data)
        return get_by_id(result_one.inserted_id)
    except errors.PyMongoError as e:
        print(f"Помилка при створенні кота: {e}")
        return None


def get_cat_by_name():
    try:
        name = input("Введіть імʼя кота для пошуку: ")
        if name:
            return list(cats_collection.find({"name": {"$regex": name}}))
        return list(cats_collection.find())
    except errors.PyMongoError as e:
        print(f"Помилка при отриманні всіх котів: {e}")
        return []


def update_cat_age():
    try:
        name = input("Введіть імʼя кота для пошуку: ")
        new_age = int(input("Введіть новий вік кота: "))
        updated_cat = cats_collection.find_one_and_update(
            {"name": name},
            {"$set": {"age": new_age}},
            return_document=True,
        )
        return updated_cat
    except errors.PyMongoError as e:
        print(f"Помилка при оновленні віку кота {name}: {e}")


def update_cat_name():
    try:
        name = input("Введіть імʼя кота для пошуку: ")
        new_name = input("Введіть нове імʼя кота: ")
        updated_cat = cats_collection.find_one_and_update(
            {"name": name},
            {"$set": {"name": new_name}},
            return_document=True,
        )
        if updated_cat:
            return updated_cat
        else:
            print(f"Кота з іменем {name} не знайдено.")
            return None
        return updated_cat

    except errors.PyMongoError as e:
        print(f"Помилка при оновленні імені кота {name}: {e}")


def add_features_by_name():
    try:
        name = input("Введіть імʼя кота для пошуку: ")
        features_input = input("Введіть особливості кота, розділені комою: ")
        new_features = [feature.strip() for feature in features_input.split(",")]

        updated_cat = cats_collection.find_one_and_update(
            {"name": name},
            {"$addToSet": {"features": {"$each": new_features}}},
            return_document=True,
        )
        if updated_cat:
            return updated_cat
        else:
            print(f"Кота з іменем {name} не знайдено.")
            return None
        return updated_cat
    except errors.PyMongoError as e:
        print(f"Помилка при додаванні характеристики до кота {name}: {e}")
        return None


def delete_by_name():
    try:
        name = input("Введіть імʼя кота для видалення: ")
        result = cats_collection.delete_one({"name": name})
        return name if result.deleted_count == 1 else None
    except errors.PyMongoError as e:
        print(f"Помилка при видаленні кота {name}: {e}")
        return None


def delete_all() -> None:
    try:
        cats_collection.delete_many({})
    except errors.PyMongoError as e:
        print(f"Помилка при видаленні всіх котів: {e}")


def exit_program():
    print("Вихід з програми...")
    exit()


def main_menu():
    while True:
        print("\nВиберіть опцію:")
        print("1. Створити запис про кота (create)")
        print("2. Отримати список всіх котів (get_all)")
        print("3. Отримати кота за ім'ям (get_cat_by_name)")
        print("4. Оновити вік кота (update_cat_age)")
        print("5. Додати характеристику коту (add_features_by_name)")
        print("6. Оновити імʼя кота (update_cat_name)")
        print("7. Видалити кота за імʼям (delete_by_name)")
        print("8. Видалити всіх котів (delete_all)")
        print("0. Вихід (exit)")

        choice = input("Ваш вибір: ")

        if choice == "1":
            print(create())
        elif choice == "2":
            print(get_all(""))
        elif choice == "3":
            print(get_cat_by_name())
        elif choice == "4":
            print(update_cat_age())
        elif choice == "5":
            print(add_features_by_name())
        elif choice == "6":
            print(update_cat_name())
        elif choice == "7":
            print(delete_by_name())
        elif choice == "8":
            print(delete_all())
        elif choice == "0":
            exit_program()


if __name__ == "__main__":
    client = create_connect()
    db = client["db-cats"]
    cats_collection = db["cats"]

    main_menu()
