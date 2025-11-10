def individual_Serial(User) -> dict:
    return {
        "id": str(User["_id"]),
        "username": User["username"],
        "email": User["email"],
        "password": User["password"],
        "subscription_tier": User.get("subscription_tier", "free"),  # Include tier with default
        "createdAt": User.get("createdAt"),  # Include creation timestamp
        "originCountryCode": User.get("originCountryCode"),  # Browser locale country code
        "originCountryName": User.get("originCountryName"),  # Browser locale country name
        "ipCountryCode": User.get("ipCountryCode"),  # IP-detected country code (actual location)
        "ipCountryName": User.get("ipCountryName")  # IP-detected country name (actual location)
    }
    
def list_serial(Users) -> list:
    return [individual_Serial(User) for User in Users]
