from fastapi import FastAPI
import pyodbc
import os
from datetime import date
from dateutil.relativedelta import relativedelta

app = FastAPI()

CONNECTION = (
    "DRIVER="
    + os.getenv("DRIVER_KA_SERVER")
    + ";SERVER="
    + os.getenv("SERVER_KA_SERVER")
    + ";PORT=1433;DATABASE="
    + os.getenv("DB_KA_SERVER")
    + ";UID="
    + os.getenv("USER_KA_SERVER")
    + ";PWD="
    + os.getenv("PW_KA_SERVER")
)


@app.get("/")
async def root():

    # return data
    return {"message": "Hello World"}


@app.get("/purchase-order/totals/")
async def get_totals(
    vendor: str = None,
    sort: str = "DESC",
    date_start: str = date.today() + relativedelta(months=-3),
    date_end: str = date.today(),
):
    cnxn = pyodbc.connect(CONNECTION)
    cursor = cnxn.cursor()
    if vendor is None:
        cursor.execute(
            f"""SELECT sku, sum(Qty) as qty_grouped FROM purchaseOrderItem where po in (SELECT po from purchaseorder where date_added >= ? and date_added <= ?) GROUP by sku ORDER by qty_grouped {sort}""",
            date_start,
            date_end,
        )
    else:
        cursor.execute(
            f"""SELECT sku, sum(Qty) as qty_grouped FROM purchaseOrderItem where po in (SELECT po from purchaseorder where vendor = ? and date_added >= ? and date_added <= ?) GROUP by sku ORDER by qty_grouped {sort}""",
            vendor,
            date_start,
            date_end,
        )
    data = [{"sku": item[0], "qty": item[1]} for item in cursor.fetchall()]
    return data
