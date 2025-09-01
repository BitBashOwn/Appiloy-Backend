from fastapi import APIRouter, HTTPException, status, Depends
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from models.users import user_collection
from utils.utils import get_current_user
from bson import ObjectId

tier_router = APIRouter()

class UpgradeTierRequest(BaseModel):
    user_id: str
    new_tier: str

@tier_router.get("/allowed-methods")
async def get_allowed_methods(current_user: dict = Depends(get_current_user)):
    """
    Get allowed methods for the current user's tier
    """
    try:
        user_tier = current_user.get("subscription_tier", "free")
        
        if user_tier == "pro":
            allowed_methods = [
                {"name": "Method 1", "description": "Follow from Hashtags"},
                {"name": "Method 2", "description": "Follow from Location"},
                {"name": "Method 3", "description": "Follow from Profile Followers"},
                {"name": "Method 4", "description": "Unfollow Non-Followers"},
                {"name": "Method 5", "description": "Follow from Profile Following"},
                {"name": "Method 6", "description": "Follow from Profile Posts"},
                {"name": "Method 7", "description": "Like Posts from Hashtags"},
                {"name": "Method 8", "description": "Comment on Posts"}
            ]
        else:
            allowed_methods = [
                {"name": "Method 4", "description": "Unfollow Non-Followers"},
                {"name": "Method 6", "description": "Follow from Profile Posts"}
            ]
        
        return JSONResponse(content={
            "tier": user_tier,
            "allowed_methods": allowed_methods
        }, status_code=200)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@tier_router.get("/user-tier")
async def get_user_tier(current_user: dict = Depends(get_current_user)):
    """
    Get the current user's tier
    """
    try:
        return JSONResponse(content={
            "user_id": current_user.get("_id"),
            "tier": current_user.get("subscription_tier", "free")
        }, status_code=200)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )

@tier_router.post("/upgrade-tier")
async def upgrade_tier(request: UpgradeTierRequest):
    """
    Upgrade user tier (for admin use or payment processing)
    """
    try:
        if request.new_tier not in ["free", "pro"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid tier. Must be 'free' or 'pro'"
            )
        
        # Update user tier in database
        result = user_collection.update_one(
            {"_id": ObjectId(request.user_id)},
            {"$set": {"subscription_tier": request.new_tier}}
        )
        
        if result.matched_count == 0:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="User not found"
            )
        
        return JSONResponse(content={
            "message": f"User tier updated to {request.new_tier}",
            "user_id": request.user_id,
            "new_tier": request.new_tier
        }, status_code=200)
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )