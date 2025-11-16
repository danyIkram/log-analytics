import time
import random
from datetime import datetime
from pathlib import Path

# # Get the directory where this script is located
# script_dir = Path(__file__).parent
# # Get the parent directory (log-analytics)
# project_root = script_dir.parent
# # Construct the log file path
# log_file = project_root / "logs" / "app.log"

# # Create logs directory if it doesn't exist
# log_file.parent.mkdir(parents=True, exist_ok=True)

# endpoints = ["/api/login", "/api/products", "/api/users", "/api/orders"]
# methods = ["INFO", "ERROR"]
# statuses = [200, 201, 400, 401, 404, 500]

# while True:
#     log = f"{datetime.now()} {random.choice(methods)} {random.choice(endpoints)} {random.choice(statuses)}"
#     with open(log_file, "a") as f:
#         f.write(log + "\n")
#     print(log)
#     time.sleep(2)


# -----------------------------
# Configuration
# -----------------------------
script_dir = Path(__file__).parent
log_file = script_dir / "logs" / "system.log"
log_file.parent.mkdir(parents=True, exist_ok=True)


months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
levels = ["INFO", "ERROR", "WARN", "DEBUG"]
components = ["sshd(pam_unix)", "kernel", "auditd", "cupsd", "systemd", "networkd"]

usernames = ["root", "admin", "guest", "test", "user"]
processes = ["sshd", "kernel", "auditd", "cupsd", "systemd", "networkd"]
ips = ["192.168.1." + str(i) for i in range(1, 255)]
memory_mb = list(range(128, 16384))
versions = [f"{i}.{j}" for i in range(1, 6) for j in range(0, 10)]



# todo ->  Example EventTemplates not complete check the linux_2k.log_template
event_templates = {
    "E16": "authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost= <*>",
    "E18": "authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost= <*> user=root",
    "E27": "check pass; user unknown",
    "E50": "HighMem zone: <*> pages, LIFO batch:<*>",
    "E54": "Initializing random number generator: succeeded",
    "E61": "Kerberos authentication failed",
    "E91": "ROOT LOGIN ON tty2",
}


# -----------------------------
# Helper functions
# -----------------------------
def random_placeholder(template):
    # Replace <*> with realistic random values
    while "<*>" in template:
        if "rhost" in template or "connection" in template:
            replacement = random.choice(ips)
        elif "user" in template:
            replacement = random.choice(usernames)
        elif "pages" in template or "memory" in template:
            replacement = str(random.choice(memory_mb))
        elif "version" in template or "v" in template:
            replacement = random.choice(versions)
        else:
            replacement = "random_val"
        template = template.replace("<*>", replacement, 1)
    return template



# -----------------------------
# Log generation
# -----------------------------
line_id = 1

while True:
    month = random.choice(months)
    date = random.randint(1, 28)
    log_time = datetime.now().strftime("%H:%M:%S")
    level = random.choice(levels)
    component = random.choice(components)
    pid = random.randint(1000, 50000)
    event_id, template = random.choice(list(event_templates.items()))
    content = random_placeholder(template)

    log_line = f"{line_id}\t{month}\t{date}\t{log_time}\t{level}\t{component}\t{pid}\t{content}\t{event_id}\t{template}"

    with open(log_file, "a") as f:
        f.write(log_line + "\n")

    print(log_line)

    line_id += 1
    time.sleep(random.uniform(0.5, 2))  # random interval between logs

