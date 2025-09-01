def individual_Serial(User) -> dict:
    return {
        "id": str(User["_id"]),
        "username": User["username"],
        "email": User["email"],
        "password": User["password"],
        "subscription_tier": User.get("subscription_tier", "free")  # Include tier with default
    }
    
def list_serial(Users) -> list:
    return [individual_Serial(User) for User in Users]
