
import psycopg
import random
from datetime import datetime, timedelta


connection = psycopg.connect(
    host="postgres",
    port=5432,
    dbname="shop",
    user="etl_user",
    password="etl_password",
)


users = [
    ("Александр Иванов", "Serbia"),
    ("Мария Петрова", "Russia"),
    ("Nikola Jovanovic", "Serbia"),
    ("Anna Schmidt", "Germany"),
    ("Jean Martin", "France"),
    ("Елена Смирнова", "Russia"),
    ("Luka Petrovic", "Serbia"),
    ("Sophie Weber", "Germany"),
    ("Pierre Dubois", "France"),
    ("Дмитрий Козлов", "Russia"),
]

products = [
    ("iPhone 16", "Electronics", 899.00),
    ("Samsung Galaxy S25", "Electronics", 799.00),
    ("Sony WH-1000XM5", "Electronics", 349.00),
    ("Logitech MX Master 3S", "Electronics", 99.00),
    ("MacBook Air M4", "Electronics", 1199.00),
    ("Clean Code", "Books", 35.00),
    ("The Pragmatic Programmer", "Books", 42.00),
    ("Atomic Habits", "Books", 22.00),
    ("1984", "Books", 15.00),
    ("Dune", "Books", 18.00),
    ("Nike Air Max", "Clothing", 130.00),
    ("Levi's 501 Jeans", "Clothing", 89.00),
    ("Adidas Hoodie", "Clothing", 65.00),
    ("North Face Jacket", "Clothing", 220.00),
    ("New Balance 574", "Clothing", 110.00),
    ("Coffee Beans 1kg", "Food", 24.00),
    ("Green Tea", "Food", 8.00),
    ("Dark Chocolate", "Food", 5.00),
    ("Olive Oil 1L", "Food", 14.00),
    ("Pasta 500g", "Food", 3.00),
]


def random_date():
    return datetime.now() - timedelta(
        days=random.randint(0, 90),
        hours=random.randint(0, 23),
        minutes=random.randint(0, 59),
    )


with connection.cursor() as cursor:

    # Users
    for i in range(100):
        name, country = random.choice(users)

        cursor.execute(
            """
            INSERT INTO users (name, country, created_at)
            VALUES (%s, %s, %s)
            """,
            (name, country, random_date()),
        )

    # Products
    for name, category, price in products:
        cursor.execute(
            """
            INSERT INTO products (name, category, price, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (name, category, price, random_date()),
        )

    # Get user IDs
    cursor.execute("SELECT id FROM users")
    user_ids = [row[0] for row in cursor.fetchall()]

    # Get product IDs and prices
    cursor.execute("SELECT id, price FROM products")
    product_prices = {
        row[0]: row[1]
        for row in cursor.fetchall()
    }

    product_ids = list(product_prices.keys())

    # Orders and order items
    for i in range(500):

        cursor.execute(
            """
            INSERT INTO orders (user_id, status, created_at)
            VALUES (%s, %s, %s)
            RETURNING id
            """,
            (
                random.choice(user_ids),
                random.choice(["pending", "completed", "cancelled"]),
                random_date(),
            ),
        )

        order_id = cursor.fetchone()[0]

        # Each order contains 1-4 products
        for product_id in random.sample(
            product_ids,
            k=random.randint(1, 4),
        ):
            quantity = random.randint(1, 5)

            cursor.execute(
                """
                INSERT INTO order_items (
                    order_id,
                    product_id,
                    quantity,
                    price
                )
                VALUES (%s, %s, %s, %s)
                """,
                (
                    order_id,
                    product_id,
                    quantity,
                    product_prices[product_id],
                ),
            )

    connection.commit()

connection.close()

print("Users, products, orders and order items inserted!")