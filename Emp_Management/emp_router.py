from fastapi import APIRouter
from .cosmos_client import get_container

router = APIRouter(prefix="/enterprise", tags=["Enterprise"])


# =========================
# 1. ALL EMPLOYEES + COUNT
# =========================
@router.get("/employees")
def get_all_employees():

    container = get_container()

    items = list(container.read_all_items())

    return {
        "total_employees": len(items),
        "employees": items
    }


# =========================
# 2. TEAM COUNT (FROM TeamData container)
# =========================
@router.get("/teams")
def get_all_teams():

    container = get_container(container_name="TeamData")

    items = list(container.read_all_items())

    return {
        "total_teams": len(items),
        "teams": items
    }


# =========================
# 3. EMPLOYEES IN A TEAM
# =========================


@router.get("/admin/{admin_id}/team-overview")
def team_overview(admin_id: str):

    container = get_container(container_name="TeamData")

    teams = list(container.query_items(
    query="SELECT * FROM c",
    enable_cross_partition_query=True
))

    result = []

    for team in teams:

        stored_admin = str(team.get("admin_ID", "")).strip().lower()
        input_admin = admin_id.strip().lower()

        if stored_admin == input_admin:

            members = team.get("members", [])
            formatted_members = []

            for m in members:

                formatted_members.append({
                    "emp_ID": m.get("emp_ID"),
                    "full_name": m.get("full_name"),
                    "level": m.get("level"),
                    "experience": m.get("experience"),
                    "active_cases": m.get("metrics", {}).get("active", 0),
                    "closed_cases": m.get("metrics", {}).get("closed", 0)
                })

            result.append({
                "team_id": team.get("team_id"),
                "team_name": team.get("team_name"),
                "members": formatted_members
            })

    return {
        "admin_id": admin_id,
        "total_teams": len(result),
        "teams": result
    }
# =========================
# 4. EMPLOYEES FULL DETAILS IN A TEAM
# =========================

@router.get("/admin/{admin_id}/employee/{emp_id}")
def get_employee(admin_id: str, emp_id: str):

    container = get_container(container_name="TeamData")

    teams = list(container.read_all_items())

    for team in teams:

        if str(team.get("admin_ID", "")).lower() != admin_id.lower():
            continue

        for member in team.get("members", []):

            if member.get("emp_ID") == emp_id:

                return {
                    "admin_id": admin_id,
                    "team_id": team.get("team_id"),
                    "employee_details": member
                }

    return {
        "status": "error",
        "message": "Employee not found under this admin"
    }

# =========================
# 5. ANALYST COUNT AND DATA
# =========================
@router.get("/employees/analysts")
def get_analysts():

    container = get_container(container_name="EmployeeData")

    items = list(container.read_all_items())

    analysts = [
        item for item in items
        if str(item.get("role", "")).lower() == "analyst"
    ]

    return {
        "analyst_count": len(analysts),
        "data": analysts
    }


# =========================
# 6. ADMIN COUNT AND DATA
# =========================
@router.get("/employees/admins")
def get_admins():

    container = get_container(container_name="EmployeeData")

    items = list(container.read_all_items())

    admins = [
        item for item in items
        if str(item.get("role", "")).lower() == "admin"
    ]

    return {
        "admin_count": len(admins),
        "data": admins
    }
