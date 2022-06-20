import uvicorn

from datetime import date, datetime
from dateutil.relativedelta import relativedelta
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from passlib.context import CryptContext
import os
import pyodbc

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

CONNECTION2 = (
    "DRIVER="
    + os.getenv("DRIVER_KA_SERVER")
    + ";SERVER="
    + os.getenv("SERVER_KA_SERVER")
    + ";PORT=1433;DATABASE="
    + os.getenv("DB_SP_KA_SERVER")
    + ";UID="
    + os.getenv("USER_KA_SERVER")
    + ";PWD="
    + os.getenv("PW_KA_SERVER")
)

app = FastAPI()

security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)


def authenticate(credentials: HTTPBasicCredentials = Depends(security)):
    cnxn = pyodbc.connect(CONNECTION)
    cursor = cnxn.cursor()
    user = cursor.execute(
        "SELECT * FROM [User] WHERE Username = ?", credentials.username
    ).fetchone()

    if not user or not verify_password(credentials.password, user.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    return {"Authorized": True}


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/vendor-imports/purchase-order/totals/")
async def get_totals(
    vendor: str = None,
    sku: str = None,
    sort: str = "DESC",
    date_start: str = date.today() + relativedelta(months=-3),
    date_end: str = date.today(),
    auth: str = Depends(authenticate),
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


@app.get("/selling-partner/purchase-orders/")
async def get_totals(
    po_number: str = None,
    date_start: str = datetime.now().replace(day=1).strftime("%m/%d/%Y"),
    date_end: str = (datetime.now().replace(day=1) + relativedelta(day=31)).strftime(
        "%m/%d/%Y"
    ),
    auth: str = Depends(authenticate),
):
    cnxn = pyodbc.connect(CONNECTION2)
    cursor = cnxn.cursor()
    if po_number is None:
        cursor.execute(
            f"""select item.po, item.[sku], track.upc, item.[unit_cost], track.[tracking_number], track.[po_date]  from [dbo].[PurchaseOrdersItems] as item join [dbo].[Tracked_Items] as track on (item.upc = track.upc and item.po = track.po ) 
    where item.po in (select po from [dbo].[PurchaseOrders] where [order_on] >= ? and [order_on] <= ? ) and item.accepted_qty > 0""",
            datetime.strptime(date_start, "%m/%d/%Y"),
            datetime.strptime(date_end, "%m/%d/%Y"),
        )
    else:
        cursor.execute(
            f"""select item.po, item.[sku], track.upc, item.[unit_cost], track.[tracking_number], track.[po_date]  from [dbo].[PurchaseOrdersItems] as item join [dbo].[Tracked_Items] as track on (item.upc = track.upc and item.po = track.po ) 
    where item.po = ?""",
            po_number,
        )

    data = [
        {
            "po": item[0],
            "sku": item[1],
            "upc": item[2],
            "unit_cost": item[3],
            "tracking_number": item[4],
            "po_date": item[5],
        }
        for item in cursor.fetchall()
    ]
    return data


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
