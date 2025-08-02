import io
from fastapi import FastAPI, Query, Request, Form
from fastapi.responses import StreamingResponse
from fastapi.templating import Jinja2Templates
from httpcore import Response
from pydantic import BaseModel
from database import execute_sql_commands, execute_sql_select_statement
import pandas as pd
from config import settings


class Beneficiary(BaseModel):
    full_name: str
    aadhar_num: str
    email_id: str
    phone_num: str
    application_num: int

class BankDetails(BaseModel):
    email_id: str
    bank_name: str
    account_num: str
    account_holder_name: str
    ifsc_code: str


class AdminCredentials(BaseModel):
    admin_username: str 
    admin_password: str

app = FastAPI()
templates = Jinja2Templates(directory = "templates")


def email_exists(email_id: str) -> bool:
    sql: str = "select * from beneficiaries where email_id = %(email_id)s;"
    
    beneficiary = execute_sql_select_statement(
        sql, 
        vars = {"email_id": email_id.lower().strip()},
        fetch_all = False
    )

    return True if beneficiary else False


@app.post("/beneficiary/create")
def create_beneficiary(beneficiary: Beneficiary):
    sql: str = """
        insert into beneficiaries(full_name, aadhar_num, email_id, phone_num, application_num)
        values(
            %(full_name)s, 
            %(aadhar_num)s, 
            %(email_id)s, 
            %(phone_num)s, 
            %(application_num)s
        )
        returning *;
    """
    new_beneficiary = execute_sql_commands(
        sql, 
        vars =  beneficiary.model_dump(),
        fetch = True
    )

    return new_beneficiary



@app.get("/beneficiary/", response_model = list[Beneficiary])
def get_beneficiaries():
    sql: str = "select * from beneficiaries;"
    
    beneficiaries = execute_sql_select_statement(
        sql, 
        fetch_all = True
    )

    return beneficiaries


@app.get("/sample")
def get_sample(request: Request):
    return templates.TemplateResponse(
        "sample.html",
        context = {
            "request": request
        }
    )


@app.get("/beneficiary/{email_id}")
def check_email_exists(
    email_id: str
):
   return email_exists(email_id.lower().strip())


@app.get("/verify_email")
def verify_email(
    request: Request,
):
    return templates.TemplateResponse(
        "bank_detail.html",
        context = {
            "request": request
        }
    )


@app.post("/bank_details/create")
def create_bank_details(
    bank_details: BankDetails
):
    if not email_exists(
        bank_details.email_id.lower().strip()
    ):
        return {
            "message": "Email ID does not exist in beneficiaries.",
            "status": "error"
        }
    
    # check if bank details already exist for the email_id
    sql: str = "select * from bank_details where email_id = %(email_id)s;"
    existing_bank_details = execute_sql_select_statement(
        sql, 
        vars = {"email_id": bank_details.email_id.lower().strip()},
        fetch_all = False
    )

    if existing_bank_details:
        return {
            "message": "Bank details already exist for this email ID.",
            "status": "error"
        }

    sql: str = """
        insert into bank_details(email_id, bank_name, account_holder_name, account_num, ifsc_code)
        values(
            %(email_id)s,
            %(bank_name)s,
            %(account_holder_name)s,
            %(account_num)s,
            %(ifsc_code)s
        )
    ;
    """
    new_bank_details = execute_sql_commands(
        sql, 
        vars = bank_details.model_dump(),
    )

    return {
        "message": "Bank details created sucessfully.",
        "status": "success"
    }


def get_bank_details():

    sql: str = """
        select 
            b.application_num,b.full_name, b.email_id, bd.bank_name, b.phone_num, bd.account_holder_name, 
            bd.account_num, bd.ifsc_code, bd.created_at
        from 
            beneficiaries b
        join 
            bank_details bd 
        on 
            b.email_id = bd.email_id;
    """
    
    bank_details = execute_sql_select_statement(
        sql,
        fetch_all = True
    )

    return bank_details


@app.get("/verify_admin")
def verify_admin(admin_credentials: AdminCredentials = Query()):
    
    if admin_credentials.admin_username == settings.admin_username and \
       admin_credentials.admin_password == settings.admin_password:
        return {"status": "success"}
    
    return {"status": "error"}


@app.get("/admin/bank_details")
def get_student_bank_details(request: Request):
  
    bank_details = get_bank_details()
    
    return bank_details



@app.get("/admin/download_bank_details")
def download_bank_details(
    request: Request
):
    bank_details = get_bank_details()

    if not bank_details:
        return {
            "message": "No bank details found.",
            "status": "error"
        }
    
    df = pd.DataFrame(bank_details)

    """Clean the DataFrame by removing non-printable characters from string columns."""
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].astype(str).replace(
            r'[\x00-\x08\x0B\x0C\x0E-\x1F]', '', regex=True
        )
    
    file = io.BytesIO()
    with pd.ExcelWriter(file, engine = "openpyxl") as writer:
        df.to_excel(writer, index = False, sheet_name = "bank_details")
    
    file.seek(0)

    # filename should contains downloading date and time
    from datetime import datetime
    current_time = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    filename = f"bank_details_{current_time}.xlsx"
    return StreamingResponse(
        file,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename=bank_details_{filename}.xlsx"}
    )


@app.get("/dashboard")
def get_dashboard(request: Request):
    return templates.TemplateResponse(
        "dashboard.html",
        context = {
            "request": request
        }
    )