"""
Step 1: Generate System Logs
This script creates realistic Linux-style system logs
"""
import time
import random
from datetime import datetime
from pathlib import Path

# Configuration
script_dir = Path(__file__).parent.parent  # Go up to log-analytics folder
log_file = script_dir / "logs" / "system.log"
log_file.parent.mkdir(parents=True, exist_ok=True)

print(f"📝 Writing logs to: {log_file}")
print("🔄 Generating logs... (Press Ctrl+C to stop)\n")

# Data for log generation
months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
levels = ["INFO", "ERROR", "WARN", "DEBUG"]
components = ["sshd", "kernel", "auditd", "cupsd", "systemd", "networkd"]
usernames = ["root", "admin", "guest", "test", "user"]
ips = ["192.168.1." + str(i) for i in range(1, 255)]
memory_mb = list(range(128, 16384))
versions = [f"{i}.{j}" for i in range(1, 6) for j in range(0, 10)]

# Event templates (simplified from Linux logs)
event_templates = {
    "E16": "authentication failure; logname= uid=0 euid=0 tty=NODEVssh ruser= rhost=<*>",
    "E18": "authentication failure; rhost=<*> user=<*>",
    "E27": "check pass; user unknown",
    "E50": "HighMem zone: <*> pages, LIFO batch:<*>",
    "E54": "Initializing random number generator: succeeded",
    "E61": "Kerberos authentication failed for user <*>",
    "E91": "ROOT LOGIN ON tty2",
    "E100": "Connection from <*> port <*>",
}

def random_placeholder(template):
    """Replace <*> placeholders with realistic random values"""
    while "<*>" in template:
        if "rhost" in template or "Connection" in template or "from" in template:
            replacement = random.choice(ips)
        elif "user" in template:
            replacement = random.choice(usernames)
        elif "pages" in template or "memory" in template:
            replacement = str(random.choice(memory_mb))
        elif "port" in template:
            replacement = str(random.randint(1024, 65535))
        elif "version" in template:
            replacement = random.choice(versions)
        else:
            replacement = str(random.randint(1, 9999))
        template = template.replace("<*>", replacement, 1)
    return template

# Main log generation loop
line_id = 1

try:
    while True:
        # Generate log components
        month = random.choice(months)
        date = random.randint(1, 28)
        log_time = datetime.now().strftime("%H:%M:%S")
        level = random.choice(levels)
        component = random.choice(components)
        pid = random.randint(1000, 50000)
        event_id, template = random.choice(list(event_templates.items()))
        content = random_placeholder(template)

        # Format: LineID\tMonth\tDate\tTime\tLevel\tComponent\tPID\tContent\tEventID\tTemplate
        log_line = f"{line_id}\t{month}\t{date}\t{log_time}\t{level}\t{component}\t{pid}\t{content}\t{event_id}\t{template}"

        # Write to file
        with open(log_file, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")

        # Print to console (show every 10th log to avoid clutter)
        if line_id % 10 == 0:
            print(f"✅ [{line_id} logs] {month} {date} {log_time} {level} {component} - {event_id}")

        line_id += 1
        time.sleep(random.uniform(0.5, 2))  # Random interval

except KeyboardInterrupt:
    print(f"\n\n🛑 Stopped. Total logs generated: {line_id - 1}")
    print(f"📄 Log file: {log_file}")