from fastapi import FastAPI, HTTPException
from auth.auth import (
    SignupRequest,
    LoginRequest,
    RoleUpdateRequest,
    signup_user,
    login_user,
    update_role,
    get_user
)
from Emp_Management.emp_router import router as employee_router

app = FastAPI(title="FRAUDPRED API")


# ================= 1. REGISTER =================
@app.post("/register")
def register(user: SignupRequest):

    result = signup_user(user)

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return {
        "message": "User registered successfully",
        "emp_id": result["emp_id"],
        "full_name": result["full_name"],
        "employee_name": result["employee_name"],
    }


# ================= 2. LOGIN =================
@app.post("/auth/login")
def login(user: LoginRequest):

    result = login_user(user.identifier, user.password)

    if not result:
        raise HTTPException(status_code=401, detail="Invalid credentials")

    return {
        "message": "Login successful",
        "emp_id": result["emp_id"],
        "full_name": result["full_name"],
        "employee_name": result["employee_name"],
        "role": result["role"]
    }


# ================= 3. ROLE CONFIGURATION =================
@app.patch("/users/{emp_id}/role")
def role_update(emp_id: str, data: RoleUpdateRequest):

    result = update_role(emp_id, data)

    if not result:
        raise HTTPException(status_code=404, detail="User not found")

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])

    return {
        "message": "Role updated successfully",
        "emp_id": emp_id,
        "extras": result.get("extras")
    }


# ================= 4. GET USER =================
@app.get("/users/{emp_id}")
def get_user_profile(emp_id: str):

    user = get_user(emp_id)

    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    return {
        "emp_id": user.get("emp_id"),
        "full_name": user.get("full_name"),
        "employee_name": user.get("employee_name"),
        "email": user.get("email"),
        "mobile_no": user.get("mobile_no"),
        "specialization": user.get("specialization"),
        "location": user.get("location"),
        "experience": user.get("experience"),
        "role": user.get("role"),
    #     "extras": user.get("extras"),
    #     "created_at": user.get("created_at")
    }



app.include_router(employee_router)
# ================= HEALTH =================
@app.get("/health")
def health():
    return {"status": "ok"}