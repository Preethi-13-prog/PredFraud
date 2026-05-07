import os
import bcrypt
from datetime import datetime
from typing import Literal, Optional, List
from dotenv import load_dotenv
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.exceptions import CosmosResourceNotFoundError
from pydantic import BaseModel, Field

load_dotenv()

# ====================== MODELS ======================

class AnalystExtra(BaseModel):
    reports_to: Optional[str] = "unassigned"
    level: Optional[str] = "-"
    assigned_queues: Optional[List[str]] = Field(default_factory=list)
    max_cases_per_day: Optional[int] = 0


class AdminExtra(BaseModel):
    admin_scope: Literal["bank", "region", "department"] = "bank"
    permissions: List[str] = Field(
        default_factory=lambda: [
            "view", "assign", "override", "configure", "create_users"
        ]
    )
    can_create_users: bool = True
    can_configure_rules: bool = True
    can_override_decisions: bool = True
    can_access_all_cases: bool = True
    team_count: int = 0


class SignupRequest(BaseModel):
    role: Literal["analyst", "admin"]
    full_name: str
    employee_name: str
    email: str
    mobile_no: str
    specialization: str
    location: str
    experience: str
    password: str


class RoleUpdateRequest(BaseModel):
    analyst_extra: Optional[AnalystExtra] = None
    admin_extra: Optional[AdminExtra] = None


class LoginRequest(BaseModel):
    identifier: str 
    password: str


# ====================== COSMOS ======================

_conn_str = os.getenv("COSMOS_CONNECTION_STRING")
_db_name = os.getenv("COSMOS_DATABASE", "fraudpred")
_container_name = os.getenv("COSMOS_CONTAINER", "EmployeeData")

_client = CosmosClient.from_connection_string(_conn_str)


def _get_db():
    try:
        db = _client.get_database_client(_db_name)
        db.read()
        return db
    except CosmosResourceNotFoundError:
        return _client.create_database_if_not_exists(id=_db_name)


def _get_container(db):
    try:
        container = db.get_container_client(_container_name)
        container.read()
        return container
    except CosmosResourceNotFoundError:
        return db.create_container_if_not_exists(
            id=_container_name,
            partition_key=PartitionKey(path="/emp_id")
        )


_db = _get_db()
_container = _get_container(_db)


# ====================== HELPERS ======================

def hash_password(pw):
    return bcrypt.hashpw(pw.encode(), bcrypt.gensalt()).decode()


def verify_password(pw, hashed):
    return bcrypt.checkpw(pw.encode(), hashed.encode())


def generate_emp_id():
    year = datetime.now().year
    users = list(_container.read_all_items())
    return f"emp-{year}-{len(users)+1:04d}"


def get_user_by_email(email):
    query = "SELECT * FROM c WHERE c.email = @email"
    params = [{"name": "@email", "value": email}]

    items = list(_container.query_items(
        query=query,
        parameters=params,
        enable_cross_partition_query=True
    ))

    return items[0] if items else None


# ====================== BUSINESS LOGIC ======================

def signup_user(user: SignupRequest):

    if get_user_by_email(user.email):
        return {"error": "Email exists"}

    emp_id = generate_emp_id()

    data = {
        "id": emp_id,
        "emp_id": emp_id,
        "full_name": user.full_name,
        "employee_name": user.employee_name,
        "email": user.email,
        "mobile_no": user.mobile_no,
        "specialization": user.specialization,
        "location": user.location,
        "experience": user.experience,
        "role": user.role,
        "hashed_password": hash_password(user.password),
        "extras": {},
        "created_at": str(datetime.utcnow())
    }

    _container.upsert_item(data)
    return data


def login_user(identifier, password):

    # CASE 1: emp_id login
    if identifier.startswith("emp-"):
        try:
            user = _container.read_item(
                item=identifier,
                partition_key=identifier
            )
        except CosmosResourceNotFoundError:
            return None

    # CASE 2: email login
    else:
        user = get_user_by_email(identifier)

    if user and verify_password(password, user["hashed_password"]):
        return user

    return None


def update_role(emp_id: str, role_data: RoleUpdateRequest):

    try:
        user = _container.read_item(item=emp_id, partition_key=emp_id)
    except CosmosResourceNotFoundError:
        return {"error": "User not found"}

    role = user.get("role")


    if role == "analyst":
        if role_data.analyst_extra:
            user["extras"] = role_data.analyst_extra.dict()

    
    elif role == "admin":
        if role_data.admin_extra:
            user["extras"] = role_data.admin_extra.dict()

    else:
        return {"error": "Invalid role"}

    _container.upsert_item(user)
    return user


def get_user(emp_id: str):
    return _container.read_item(item=emp_id, partition_key=emp_id)
