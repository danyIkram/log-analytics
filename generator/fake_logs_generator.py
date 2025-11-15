import time
import random
from datetime import datetime
from pathlib import Path

# Get the directory where this script is located
script_dir = Path(__file__).parent
# Get the parent directory (log-analytics)
project_root = script_dir.parent
# Construct the log file path
log_file = project_root / "logs" / "app.log"

# Create logs directory if it doesn't exist
log_file.parent.mkdir(parents=True, exist_ok=True)

endpoints = ["/api/login", "/api/products", "/api/users", "/api/orders"]
methods = ["INFO", "ERROR"]
statuses = [200, 201, 400, 401, 404, 500]

while True:
    log = f"{datetime.now()} {random.choice(methods)} {random.choice(endpoints)} {random.choice(statuses)} generated log"
    with open(log_file, "a") as f:
        f.write(log + "\n")
    print(log)
    time.sleep(2)
