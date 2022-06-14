from fastapi import FastAPI
import pyodbc
from config import CONNECTION, get_all_tracking
from datetime import date
from dateutil.relativedelta import relativedelta

app = FastAPI()


@app.get("/")
async def root():
    cnxn = pyodbc.connect(CONNECTION)
    cursor = cnxn.cursor()
    cursor.execute(get_all_tracking)
    data = [(item[0]) for item in cursor.fetchall()]
    return data
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
