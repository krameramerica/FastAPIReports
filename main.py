import uvicorn

from datetime import date
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


@app.get("/purchase-order/totals/")
async def get_totals(
    vendor: str = None,
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


# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)
