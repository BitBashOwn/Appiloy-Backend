# Appilot Backend API

**Appilot** is a comprehensive automation platform that enables users to manage and control multiple Android devices remotely for social media automation tasks. This backend API serves as the core engine for device management, task scheduling, user authentication, and real-time communication between web clients and Android devices.

## 🚀 Features

### Core Functionality
- **Device Management**: Register, monitor, and control multiple Android devices remotely
- **Task Automation**: Create, schedule, and manage automation tasks for social media platforms
- **Real-time Communication**: WebSocket-based communication with Android devices
- **User Authentication**: Secure JWT-based authentication and authorization
- **Bot Integration**: Built-in Discord bot for notifications and status updates
- **Task Scheduling**: Advanced scheduling system with support for recurring tasks
- **Multi-worker Architecture**: Distributed system with Redis-based coordination

### Supported Platforms
- Instagram automation (following, unfollowing, engagement)
- Multi-account management
- Custom automation workflows

## 🏗️ Architecture

### System Components

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Frontend      │    │   Backend API   │    │   Android App   │
│   (React/Vue)   │◄──►│   (FastAPI)     │◄──►│   (WebSocket)   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                              │
                              ▼
                       ┌─────────────────┐
                       │   MongoDB       │
                       │   Database      │
                       └─────────────────┘
                              │
                       ┌─────────────────┐
                       │   Redis         │
                       │   (Pub/Sub)     │
                       └─────────────────┘
                              │
                       ┌─────────────────┐
                       │   Discord Bot   │
                       │   (Notifications)│
                       └─────────────────┘
```

### Key Components

- **FastAPI Application**: RESTful API with automatic documentation
- **WebSocket Server**: Real-time bidirectional communication with devices
- **Task Scheduler**: APScheduler for job management and recurring tasks
- **Redis Integration**: Worker coordination and message queuing
- **MongoDB Database**: Data persistence and user management
- **Discord Bot**: Automated notifications and status updates

## 📋 Prerequisites

- Python 3.8+
- MongoDB Atlas account (or local MongoDB instance)
- Redis server
- Discord Bot Token (for notifications)
- Email service credentials (for password reset)

## 🛠️ Installation

### 1. Clone the Repository
```bash
git clone https://github.com/AbdullahNoor-codes/Appiloy-Console-Backend.git
cd Appiloy-Console-Backend
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Environment Configuration
Create a `.env` file in the root directory:

```env
# Database Configuration
db_uri=mongodb+srv://username:password@cluster.mongodb.net/?retryWrites=true&w=majority&appName=Appilot

# Redis Configuration
REDIS_URL=redis://localhost:6379

# JWT Configuration
JWT_SECRET_KEY=your_super_secret_jwt_key_here
JWT_ALGORITHM=HS256
JWT_EXPIRY_HOURS=24

# Discord Bot Configuration
DISCORD_BOT_TOKEN=your_discord_bot_token_here

# Email Configuration (for password reset)
RESEND_API_KEY=your_resend_api_key_here
FROM_EMAIL=noreply@yourdomain.com

# Application Configuration
ENVIRONMENT=development
LOG_LEVEL=INFO
WORKER_ID=worker_1
```

### 5. Start the Application

#### Development Mode
```bash
python main.py
```

#### Production Mode
```bash
gunicorn -c gunicorn_conf.py main:app
```

The API will be available at `http://localhost:8000`

## 📚 API Documentation

### Interactive Documentation
- **Swagger UI**: `http://localhost:8000/docs`
- **ReDoc**: `http://localhost:8000/redoc`

### Core Endpoints

#### Authentication
```
POST /login          - User login
POST /signup         - User registration
POST /reset-password - Password reset request
POST /verify-reset   - Verify password reset
```

#### Device Management
```
POST /register_device              - Register new Android device
GET  /device_status/{device_id}    - Check device status
PUT  /update_status/{device_id}    - Update device status
GET  /get-devices                  - Get user's devices
DELETE /delete-devices             - Remove devices
```

#### Task Management
```
POST /create-task        - Create new automation task
POST /create-task-copy   - Copy existing task
GET  /get-task          - Get specific task
GET  /get-all-task      - Get all user tasks
GET  /get-scheduled-tasks - Get scheduled tasks
GET  /get-running-tasks   - Get currently running tasks
PUT  /update-task        - Update task configuration
DELETE /delete-tasks     - Delete tasks
```

#### Bot Management
```
GET /get-bots           - Get available automation bots
GET /get-bot            - Get specific bot details
```

#### Device Control
```
POST /send_command      - Send automation commands to devices
POST /stop_task         - Stop running tasks
PUT  /clear-old-jobs    - Clean up old scheduled jobs
```

### WebSocket Endpoints
```
WS /ws/{device_id}      - Device communication channel
```

## 🔧 Configuration

### Database Schema

#### Users Collection
```javascript
{
  "_id": ObjectId,
  "name": String,
  "email": String,
  "password": String,  // Hashed
  "createdAt": Date
}
```

#### Devices Collection
```javascript
{
  "_id": ObjectId,
  "deviceName": String,
  "deviceId": String,
  "model": String,
  "botName": [String],
  "status": Boolean,
  "activationDate": String,
  "email": String
}
```

#### Tasks Collection
```javascript
{
  "_id": ObjectId,
  "id": String,
  "email": String,
  "taskName": String,
  "status": String,  // "awaiting", "scheduled", "running"
  "bot": String,
  "isScheduled": Boolean,
  "activeJobs": [Object],
  "inputs": Object,
  "LastModifiedDate": Date,
  "activationDate": Date,
  "deviceIds": [String],
  "serverId": String,
  "channelId": String
}
```

### Task Scheduling Types

1. **ExactStartTime**: Start at specific time
2. **DurationWithTimeWindow**: Random start within time window
3. **EveryDayAutomaticRun**: Daily recurring execution

## 🔌 WebSocket Communication

### Device Connection Flow
1. Device connects to `/ws/{device_id}`
2. Device registration in Redis
3. Status update in MongoDB
4. Heartbeat/ping-pong mechanism

### Message Types
- **ping/pong**: Heartbeat mechanism
- **update**: Progress updates from device
- **final**: Task completion notification
- **error**: Error reporting

### Example WebSocket Message
```json
{
  "type": "update",
  "message": "Following user @example...",
  "task_id": "task_123",
  "job_id": "job_456",
  "timestamp": 1634567890
}
```

## 🤖 Bot Integration

### Discord Bot Features
- Task completion notifications
- Error alerts
- Schedule confirmations
- Real-time status updates

### Bot Commands
The Discord bot automatically sends notifications to configured channels when:
- Tasks are scheduled
- Tasks complete successfully
- Errors occur during execution
- Devices disconnect

## 📈 Monitoring & Logging

### Logging Configuration
The application uses structured logging with different levels:
- **INFO**: General application flow
- **WARNING**: Recoverable issues
- **ERROR**: Serious problems
- **DEBUG**: Detailed debugging information

### Health Checks
```bash
GET /  # Returns {"message": "running"}
```

### Redis Monitoring
- Worker status tracking
- Device connection registry
- Command distribution monitoring

## 🔒 Security Features

### Authentication
- JWT-based authentication
- Password hashing with bcrypt
- Token expiration management

### Authorization
- User-specific resource access
- Device ownership validation
- Task permission checks

### CORS Configuration
Configured to allow requests from:
- Local development (`localhost:5173`)
- Production frontend domains

## 🚢 Deployment

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
EXPOSE 8000

CMD ["gunicorn", "-c", "gunicorn_conf.py", "main:app"]
```

### Environment Variables for Production
```env
ENVIRONMENT=production
LOG_LEVEL=WARNING
REDIS_URL=redis://redis-service:6379
db_uri=mongodb+srv://prod-user:password@prod-cluster.mongodb.net/
```

### Scaling Considerations
- Multiple worker instances with Redis coordination
- Database connection pooling
- Load balancing for WebSocket connections

## 🧪 Testing

### Running Tests
```bash
python -m pytest tests/
```

### Test Coverage
- Unit tests for core business logic
- Integration tests for API endpoints
- WebSocket connection testing

## 🤝 Contributing

### Development Setup
1. Fork the repository
2. Create feature branch: `git checkout -b feature-name`
3. Make changes and add tests
4. Ensure code follows PEP 8 standards
5. Submit pull request

### Code Style
- Use Black for code formatting
- Follow PEP 8 guidelines
- Add type hints where appropriate
- Include docstrings for functions

## 📋 Troubleshooting

### Common Issues

#### MongoDB Connection Issues
```bash
# Check MongoDB URI format
# Ensure network access is configured
# Verify credentials
```

#### Redis Connection Problems
```bash
# Verify Redis server is running
# Check Redis URL format
# Ensure Redis is accessible
```

#### WebSocket Connection Failures
```bash
# Check device ID format
# Verify network connectivity
# Review WebSocket logs
```

### Debug Mode
Enable debug logging by setting:
```env
LOG_LEVEL=DEBUG
```

## 📊 Performance Optimization

### Database Optimization
- Index frequently queried fields
- Use projection to limit returned fields
- Implement connection pooling

### Redis Optimization
- Set appropriate TTL for cached data
- Use Redis pipelining for bulk operations
- Monitor memory usage

### WebSocket Optimization
- Implement connection pooling
- Use compression for large messages
- Handle reconnection gracefully

## 🛣️ Roadmap

### Upcoming Features
- [ ] Multi-platform support (TikTok, Twitter)
- [ ] Advanced analytics dashboard
- [ ] Machine learning-based optimization
- [ ] Mobile app management interface
- [ ] Advanced scheduling algorithms

### Performance Improvements
- [ ] Database query optimization
- [ ] Caching layer implementation
- [ ] Horizontal scaling support
- [ ] Real-time metrics collection

## 📄 License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

## 📞 Support

For support and questions:
- **Email**: support@appilot.com
- **GitHub Issues**: [Create an issue](https://github.com/AbdullahNoor-codes/Appiloy-Console-Backend/issues)
- **Documentation**: [Wiki](https://github.com/AbdullahNoor-codes/Appiloy-Console-Backend/wiki)

## 🙏 Acknowledgments

- FastAPI team for the excellent framework
- MongoDB team for the database solution
- Redis team for the caching and pub/sub system
- Discord.py community for bot integration support

---

**Made with ❤️ by the Appilot Team**
