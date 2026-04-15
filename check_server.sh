#!/bin/bash

# Check if the server is running

echo "🔍 Checking server status..."
echo ""

# Check if Flask process is running
if pgrep -f "python.*app.py" > /dev/null; then
    echo "✅ Flask server is RUNNING"
    echo ""
    echo "Process details:"
    ps aux | grep -i "python.*app.py" | grep -v grep
    echo ""
    echo "🌐 Server should be accessible at: http://localhost:5000"
else
    echo "❌ Flask server is NOT running"
    echo ""
    echo "To start the server, run:"
    echo "  ./start_server.sh"
    echo ""
    echo "Or manually:"
    echo "  python3 app.py"
fi

echo ""
echo "🔌 Testing server connection..."
if curl -s http://localhost:5000/api/status > /dev/null 2>&1; then
    echo "✅ Server is responding to requests"
else
    echo "❌ Server is not responding"
    echo ""
    echo "Possible issues:"
    echo "  1. Server is not running (start with: python3 app.py)"
    echo "  2. Port 5000 is blocked or in use"
    echo "  3. Firewall is blocking connections"
fi
