from connect_db import create_connect
from pymongo import errors
from bson.objectid import ObjectId

client = create_connect()
db = client["db-cats"]
cats_collection = db["cats"]


def get_all(name: str | None = None) -> list[dict]:
    try:
        if name:
            res = list(cats_collection.find({"name": {"$regex": name}}))
            if res:
                return res
            else:
                return f"Кота з іменем {name} не знайдено."
        return list(cats_collection.find())
    except errors.PyMongoError as e:
        return f"Помилка при отриманні всіх котів: {e}"


def get_by_id(id: ObjectId) -> dict | None:
    try:
        return cats_collection.find_one({"_id": id})
    except errors.PyMongoError as e:
        return f"Помилка при отриманні кота за ID {id}: {e}"


def create():
    try:
        name = input("Введіть імʼя кота для створення: ").strip()
        age = int(input("Введіть вік кота: "))
        features_input = input("Введіть особливості кота, розділені комою: ")
        features = [
            feature.strip() for feature in features_input.split(",") if feature.strip()
        ]

        data = {"name": name, "age": age, "features": features}

        result_one = cats_collection.insert_one(data)
        return get_by_id(result_one.inserted_id)
    except errors.PyMongoError as e:
        return f"Помилка при створенні кота: {e}"


def update_cat_age(name: str) -> dict | None:
    try:
        new_age = int(input("Введіть новий вік кота: "))
        updated_cat = cats_collection.find_one_and_update(
            {"name": name},
            {"$set": {"age": new_age}},
            return_document=True,
        )
        if updated_cat:
            return updated_cat
        else:
            return f"Кота з іменем {name} не знайдено."

    except errors.PyMongoError as e:
        print(f"Помилка при оновленні віку кота {name}: {e}")


def update_cat_name(name: str) -> dict | None:
    try:
        new_name = input("Введіть нове імʼя кота: ")
        updated_cat = cats_collection.find_one_and_update(
            {"name": name},
            {"$set": {"name": new_name}},
            return_document=True,
        )
        if updated_cat:
            return updated_cat
        else:
            return f"Кота з іменем {name} не знайдено."

    except errors.PyMongoError as e:
        print(f"Помилка при оновленні імені кота {name}: {e}")


def add_features_by_name(name: str) -> dict | None:
    try:
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
            return f"Кота з іменем {name} не знайдено."

    except errors.PyMongoError as e:
        return f"Помилка при додаванні характеристики до кота {name}: {e}"


def delete_by_name(name: str) -> dict | None:
    try:
        result = cats_collection.delete_one({"name": name})
        return name if result.deleted_count == 1 else None
    except errors.PyMongoError as e:
        return f"Помилка при видаленні кота {name}: {e}"


def delete_all() -> None:
    try:
        cats_collection.delete_many({})
    except errors.PyMongoError as e:
        return f"Помилка при видаленні всіх котів: {e}"


def exit_program():
    print("Вихід з програми...")
    exit()


def get_name() -> str:
    while True:
        name = input("Введіть імʼя кота для пошуку: ").strip()
        try:
            res = cats_collection.find_one({"name": name})
            if res:
                return name
            else:
                print(f"Кота {name} не знайдено.")
                pass
        except errors.PyMongoError as e:
            print(f"Помилка при отриманні кота за ім'ям {name}: {e}")


def print_cats(cats: list | str) -> None:
    if not isinstance(cats, list):
        print(cats)
        return

    for cat in cats:
        print(cat)


def main_menu():
    print("\nВиберіть опцію або 0. Вихід (exit):")
    print("1. Створити запис про кота (create)")
    print("2. Отримати список всіх котів (get_all)")
    print("3. Отримати кота за ім'ям (get_all(name)")
    print("4. Оновити вік кота (update_cat_age)")
    print("5. Додати характеристику коту (add_features_by_name)")
    print("6. Оновити імʼя кота (update_cat_name)")
    print("7. Видалити кота за імʼям (delete_by_name)")
    print("8. Видалити всіх котів (delete_all)")

    while True:

        choice = input("Ваш вибір: ")

        if choice == "0":
            exit_program()
        elif choice == "1":
            print(create())
        elif choice == "2":
            print_cats(get_all())
        elif choice == "3":
            print(get_all(get_name()))
        elif choice == "4":
            print(update_cat_age(get_name()))
        elif choice == "5":
            print(add_features_by_name(get_name()))
        elif choice == "6":
            print(update_cat_name(get_name()))
        elif choice == "7":
            print(delete_by_name(get_name()))
        elif choice == "8":
            print(delete_all())


if __name__ == "__main__":
    main_menu()
