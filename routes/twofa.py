"""
2FA Service Router for FastAPI Backend
Provides REST API for generating TOTP codes from secret keys using pyotp
This replaces the standalone Flask 2fa_service.py and integrates into FastAPI
"""

import pyotp
import time
from fastapi import APIRouter, HTTPException
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from datetime import datetime
from logger import logger

# Create router with prefix
twofa_router = APIRouter(prefix="/api/2fa", tags=["2FA"])

class TwoFactorAuthService:
    """2FA Service using pyotp library"""
    
    def __init__(self):
        self.totp_cache = {}  # Cache for TOTP objects
        logger.info("🔐 2FA Service initialized")
    
    def generate_otp(self, secret_key: str) -> dict:
        """
        Generate OTP code from 2FA secret key
        
        Args:
            secret_key (str): The 2FA secret key (32 characters)
        
        Returns:
            dict: Response with OTP code and metadata
        """
        logger.info("=" * 60)
        logger.info("🔐 STARTING OTP GENERATION PROCESS")
        logger.info("=" * 60)
        logger.info(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"🔑 Input Secret Key: {secret_key[:8]}...{secret_key[-8:]}")
        logger.info(f"📏 Secret Key Length: {len(secret_key)} characters")
        
        try:
            # Clean the secret key
            original_key = secret_key
            secret_key = secret_key.replace(" ", "").upper().strip()
            logger.info(f"🧹 Cleaned Secret Key: {secret_key[:8]}...{secret_key[-8:]}")
            
            # Validate secret key length
            if len(secret_key) != 32:
                logger.error(f"❌ INVALID SECRET KEY LENGTH: Expected 32, got {len(secret_key)}")
                return {
                    "success": False,
                    "error": f"Invalid secret key length. Expected 32 characters, got {len(secret_key)}",
                    "secret_key": secret_key
                }
            logger.info("✅ Secret key length validation passed")
            
            # Validate secret key format (Base32)
            try:
                logger.info("🔍 Validating Base32 format...")
                import base64
                base64.b32decode(secret_key)
                logger.info("✅ Base32 format validation passed")
            except Exception as e:
                logger.error(f"❌ INVALID BASE32 FORMAT: {str(e)}")
                return {
                    "success": False,
                    "error": f"Invalid Base32 format: {str(e)}",
                    "secret_key": secret_key
                }
            
            # Create or get cached TOTP object
            logger.info("🔄 Checking TOTP object cache...")
            if secret_key not in self.totp_cache:
                logger.info("📦 Creating new TOTP object (not in cache)")
                self.totp_cache[secret_key] = pyotp.TOTP(secret_key)
                logger.info("✅ TOTP object created successfully")
            else:
                logger.info("♻️ Using cached TOTP object")
            
            totp = self.totp_cache[secret_key]
            
            # Generate current OTP
            logger.info("⚡ Generating OTP code using pyotp library...")
            otp_code = totp.now()
            logger.info(f"🎯 Generated OTP Code: {otp_code}")
            
            # Calculate time remaining
            current_time = int(time.time())
            time_remaining = 30 - (current_time % 30)
            logger.info(f"⏰ Current Time: {current_time}")
            logger.info(f"⏳ Time Remaining: {time_remaining} seconds")
            
            # Log success details
            logger.info("=" * 60)
            logger.info("✅ OTP GENERATION SUCCESSFUL!")
            logger.info("=" * 60)
            logger.info(f"🔑 Secret Key: {secret_key[:8]}...{secret_key[-8:]}")
            logger.info(f"🔢 Generated OTP: {otp_code}")
            logger.info(f"⏳ Time Remaining: {time_remaining} seconds")
            logger.info(f"📅 Timestamp: {current_time}")
            logger.info(f"🔄 Algorithm: TOTP")
            logger.info(f"⏱️ Period: 30 seconds")
            logger.info("=" * 60)
            
            return {
                "success": True,
                "otp_code": otp_code,
                "secret_key": secret_key,
                "time_remaining": time_remaining,
                "timestamp": current_time,
                "algorithm": "TOTP",
                "period": 30
            }
            
        except Exception as e:
            logger.error("=" * 60)
            logger.error("❌ OTP GENERATION FAILED!")
            logger.error("=" * 60)
            logger.error(f"🚨 Error: {str(e)}")
            logger.error(f"🔑 Secret Key: {secret_key[:8] if 'secret_key' in locals() else 'Unknown'}...{secret_key[-8:] if 'secret_key' in locals() else 'Unknown'}")
            logger.error(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            logger.error("=" * 60)
            return {
                "success": False,
                "error": f"Error generating OTP: {str(e)}",
                "secret_key": secret_key if 'secret_key' in locals() else None
            }
    
    def verify_otp(self, secret_key: str, otp_code: str) -> dict:
        """
        Verify an OTP code against a secret key
        
        Args:
            secret_key (str): The 2FA secret key
            otp_code (str): The OTP code to verify
        
        Returns:
            dict: Response with verification result
        """
        try:
            # Clean the secret key
            secret_key = secret_key.replace(" ", "").upper().strip()
            
            # Create TOTP object
            totp = pyotp.TOTP(secret_key)
            
            # Verify the OTP
            is_valid = totp.verify(otp_code)
            
            return {
                "success": True,
                "valid": is_valid,
                "secret_key": secret_key,
                "otp_code": otp_code,
                "timestamp": int(time.time())
            }
            
        except Exception as e:
            logger.error(f"Error verifying OTP: {str(e)}")
            return {
                "success": False,
                "error": f"Error verifying OTP: {str(e)}",
                "secret_key": secret_key if 'secret_key' in locals() else None
            }


# Initialize the service
tfa_service = TwoFactorAuthService()


# Pydantic models for request validation
class GenerateOTPRequest(BaseModel):
    secret_key: str


class VerifyOTPRequest(BaseModel):
    secret_key: str
    otp_code: str


@twofa_router.get("/health")
async def health_check():
    """Health check endpoint"""
    logger.info("🏥 HEALTH CHECK REQUEST RECEIVED")
    logger.info(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("✅ Service is healthy and running")
    return JSONResponse(content={
        "status": "healthy",
        "service": "2FA Service (Integrated FastAPI)",
        "timestamp": int(time.time())
    })


@twofa_router.post("/generate")
async def generate_otp(request: GenerateOTPRequest):
    """Generate OTP code from secret key (POST method)"""
    logger.info("=" * 80)
    logger.info("📨 POST /api/2fa/generate REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🔑 Secret Key from POST: {request.secret_key[:8]}...{request.secret_key[-8:]}")
    
    try:
        result = tfa_service.generate_otp(request.secret_key)
        
        if result['success']:
            logger.info("✅ POST /generate REQUEST COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            return JSONResponse(content=result, status_code=200)
        else:
            logger.error("❌ POST /generate REQUEST FAILED")
            logger.error("=" * 80)
            return JSONResponse(content=result, status_code=400)
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 POST /generate SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error("=" * 80)
        return JSONResponse(content={
            "success": False,
            "error": f"Server error: {str(e)}"
        }, status_code=500)


@twofa_router.get("/generate/{secret_key}")
async def generate_otp_get(secret_key: str):
    """Generate OTP code from secret key (GET method for Android bot)"""
    logger.info("=" * 80)
    logger.info("📨 GET /api/2fa/generate/<secret_key> REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🔑 Secret Key from URL: {secret_key[:8]}...{secret_key[-8:]}")
    
    try:
        result = tfa_service.generate_otp(secret_key)
        
        if result['success']:
            logger.info("✅ GET /generate REQUEST COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            return JSONResponse(content=result, status_code=200)
        else:
            logger.error("❌ GET /generate REQUEST FAILED")
            logger.error("=" * 80)
            return JSONResponse(content=result, status_code=400)
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 GET /generate SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        return JSONResponse(content={
            "success": False,
            "error": f"Server error: {str(e)}"
        }, status_code=500)


@twofa_router.post("/verify")
async def verify_otp(request: VerifyOTPRequest):
    """Verify OTP code against secret key"""
    logger.info("=" * 80)
    logger.info("🔍 POST /api/2fa/verify REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🔑 Secret Key: {request.secret_key[:8]}...{request.secret_key[-8:]}")
    logger.info(f"🔢 OTP Code: {request.otp_code}")
    
    try:
        result = tfa_service.verify_otp(request.secret_key, request.otp_code)
        
        if result.get('valid'):
            logger.info("✅ OTP VERIFICATION SUCCESSFUL")
        else:
            logger.warning("⚠️ OTP VERIFICATION FAILED")
        
        logger.info("=" * 80)
        return JSONResponse(content=result, status_code=200)
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 POST /verify SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        return JSONResponse(content={
            "success": False,
            "error": f"Server error: {str(e)}"
        }, status_code=500)


# Log initialization
logger.info("=" * 80)
logger.info("🚀 2FA SERVICE ROUTER INITIALIZED")
logger.info("=" * 80)
logger.info("🔧 Service: Integrated FastAPI 2FA Router (pyotp)")
logger.info("📋 Endpoints:")
logger.info("   GET  /api/2fa/health                    - Health check")
logger.info("   POST /api/2fa/generate                  - Generate OTP (JSON body)")
logger.info("   GET  /api/2fa/generate/<secret_key>     - Generate OTP (URL param)")
logger.info("   POST /api/2fa/verify                    - Verify OTP")
logger.info("=" * 80)
logger.info("✅ 2FA Service ready to receive requests from Android bot")
logger.info("=" * 80)
