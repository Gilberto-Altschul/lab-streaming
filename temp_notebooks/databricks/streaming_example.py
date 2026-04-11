# Databricks notebook source
# MAGIC %md
# MAGIC # Exemplo de notebook Databricks
# MAGIC
# MAGIC Este notebook está versionado no Git e pode ser deployado no workspace Databricks.

# COMMAND ----------
from datetime import datetime, timezone
import random
import json

STATUSES = ["created", "paid", "shipped"]
SOURCES = ["web", "mobile", "api"]
SKUS = ["helmet_red", "cap_black", "jacket_team", "gloves_pro"]


def create_order_event(counter: int) -> dict:
    unit_price = round(random.uniform(20, 500), 2)
    qty = random.randint(1, 3)
    return {
        "order_id": f"ord_{counter:05d}",
        "customer_id": f"cust_{random.randint(1, 20):03d}",
        "amount": round(unit_price * qty, 2),
        "currency": "BRL",
        "status": random.choice(STATUSES),
        "items": [{
            "sku": random.choice(SKUS),
            "qty": qty,
            "unit_price": unit_price,
        }],
        "source": random.choice(SOURCES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


# COMMAND ----------
order = create_order_event(1)
print(json.dumps(order, indent=2))
