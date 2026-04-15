#!/bin/bash

# Start the ISBN scraper web server

echo "🚀 Starting ISBN Scraper Web Server..."
echo ""

# Check if MongoDB is accessible
echo "📊 Checking MongoDB connection..."
python3 test_db_connection.py

echo ""
echo "🌐 Starting Flask server on http://localhost:5000"
echo "📝 Press Ctrl+C to stop the server"
echo ""

# Start Flask app
python3 app.py
