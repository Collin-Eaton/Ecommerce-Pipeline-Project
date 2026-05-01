from fastapi import FastAPI
from faker import Faker
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup Logic ---
    print("Starting up: Initialize resources")
    # Example: database.connect()
    yield
    # --- Shutdown Logic ---
    print("Shutting down: Clean up resources")

app = FastAPI(lifespan=lifespan)
fake = Faker()

customers = []

# generate fake data
for i in range(10):
    customers.append({
        "customer_id": i,
        "name": fake.name(),
        "email": fake.email()
    })

@app.get("/customers")
def get_customers():
    return customers

@app.get("/products")
def get_products():
    return [
        {"product_id": 1, "name": "Mattress", "price": 999},
        {"product_id": 2, "name": "Pillow", "price": 99}
    ]

import random
from datetime import datetime

orders = []

for i in range(20):
    orders.append({
        "order_id": i,
        "customer_id": random.randint(0, 9),
        "order_date": datetime.now().isoformat(),
        "total_amount": round(random.uniform(100, 2000), 2)
    })

@app.get("/orders")
def get_orders(limit: int = 5):
    return orders[:limit]

returns = []

for i in range(5):
    returns.append({
        "return_id": i,
        "order_id": i,
        "reason": "too firm"
    })

@app.get("/returns")
def get_returns():
    return returns
