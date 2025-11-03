#!/usr/bin/env python3
"""
2FA Service using pyotp library
Provides REST API for generating TOTP codes from secret keys
"""

import pyotp
import json
import time
from flask import Flask, request, jsonify
from flask_cors import CORS
import logging
from datetime import datetime

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
CORS(app)  # Enable CORS for cross-origin requests

class TwoFactorAuthService:
    """2FA Service using pyotp library"""
    
    def __init__(self):
        self.totp_cache = {}  # Cache for TOTP objects
    
    def generate_otp(self, secret_key):
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
    
    def verify_otp(self, secret_key, otp_code):
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

@app.route('/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    logger.info("🏥 HEALTH CHECK REQUEST RECEIVED")
    logger.info(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("✅ Service is healthy and running")
    return jsonify({
        "status": "healthy",
        "service": "2FA Service",
        "timestamp": int(time.time())
    })

@app.route('/generate', methods=['POST'])
def generate_otp():
    """Generate OTP code from secret key"""
    logger.info("=" * 80)
    logger.info("📨 POST /generate REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🌐 Client IP: {request.remote_addr}")
    logger.info(f"📋 Content-Type: {request.content_type}")
    
    try:
        data = request.get_json()
        logger.info(f"📦 Request Data: {data}")
        
        if not data or 'secret_key' not in data:
            logger.error("❌ MISSING SECRET_KEY IN REQUEST BODY")
            return jsonify({
                "success": False,
                "error": "Missing 'secret_key' in request body"
            }), 400
        
        secret_key = data['secret_key']
        logger.info(f"🔑 Secret Key from POST: {secret_key[:8]}...{secret_key[-8:]}")
        
        result = tfa_service.generate_otp(secret_key)
        
        if result['success']:
            logger.info("✅ POST /generate REQUEST COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            return jsonify(result), 200
        else:
            logger.error("❌ POST /generate REQUEST FAILED")
            logger.error("=" * 80)
            return jsonify(result), 400
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 POST /generate SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error(f"📅 Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.error("=" * 80)
        return jsonify({
            "success": False,
            "error": f"Server error: {str(e)}"
        }), 500

@app.route('/verify', methods=['POST'])
def verify_otp():
    """Verify OTP code against secret key"""
    logger.info("=" * 80)
    logger.info("🔍 POST /verify REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🌐 Client IP: {request.remote_addr}")
    
    try:
        data = request.get_json()
        logger.info(f"📦 Request Data: {data}")
        
        if not data or 'secret_key' not in data or 'otp_code' not in data:
            logger.error("❌ MISSING SECRET_KEY OR OTP_CODE IN REQUEST BODY")
            return jsonify({
                "success": False,
                "error": "Missing 'secret_key' or 'otp_code' in request body"
            }), 400
        
        secret_key = data['secret_key']
        otp_code = data['otp_code']
        logger.info(f"🔑 Secret Key: {secret_key[:8]}...{secret_key[-8:]}")
        logger.info(f"🔢 OTP Code: {otp_code}")
        
        result = tfa_service.verify_otp(secret_key, otp_code)
        
        if result['valid']:
            logger.info("✅ OTP VERIFICATION SUCCESSFUL")
        else:
            logger.warning("⚠️ OTP VERIFICATION FAILED")
        
        logger.info("=" * 80)
        return jsonify(result), 200
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 POST /verify SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        return jsonify({
            "success": False,
            "error": f"Server error: {str(e)}"
        }), 500

@app.route('/generate/<secret_key>', methods=['GET'])
def generate_otp_get(secret_key):
    """Generate OTP code from secret key (GET method for testing)"""
    logger.info("=" * 80)
    logger.info("📨 GET /generate/<secret_key> REQUEST RECEIVED")
    logger.info("=" * 80)
    logger.info(f"📅 Request Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"🌐 Client IP: {request.remote_addr}")
    logger.info(f"🔑 Secret Key from URL: {secret_key[:8]}...{secret_key[-8:]}")
    
    try:
        result = tfa_service.generate_otp(secret_key)
        
        if result['success']:
            logger.info("✅ GET /generate REQUEST COMPLETED SUCCESSFULLY")
            logger.info("=" * 80)
            return jsonify(result), 200
        else:
            logger.error("❌ GET /generate REQUEST FAILED")
            logger.error("=" * 80)
            return jsonify(result), 400
            
    except Exception as e:
        logger.error("=" * 80)
        logger.error("🚨 GET /generate SERVER ERROR")
        logger.error("=" * 80)
        logger.error(f"🚨 Error: {str(e)}")
        logger.error("=" * 80)
        return jsonify({
            "success": False,
            "error": f"Server error: {str(e)}"
        }), 500

if __name__ == '__main__':
    logger.info("=" * 80)
    logger.info("🚀 STARTING 2FA SERVICE WITH DETAILED LOGGING")
    logger.info("=" * 80)
    logger.info(f"📅 Startup Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("🔧 Service: Python pyotp 2FA Service")
    logger.info("🌐 Host: 0.0.0.0")
    logger.info("🔌 Port: 5000")
    logger.info("📋 Endpoints:")
    logger.info("   GET  /health                    - Health check")
    logger.info("   POST /generate                  - Generate OTP (JSON body)")
    logger.info("   GET  /generate/<secret_key>     - Generate OTP (URL param)")
    logger.info("   POST /verify                    - Verify OTP")
    logger.info("=" * 80)
    logger.info("✅ Service ready to receive requests from Java bot")
    logger.info("=" * 80)
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=5000, debug=True)