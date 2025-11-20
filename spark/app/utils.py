import re
from datetime import datetime

LOG_PATTERN = re.compile(
    r"(\w+)\s+(\w+)\s+(\d+)\s+(\d+:\d+:\d+)\s+(\w+)\s+([\w()_]+)\s+(\d+)\s+(.*)"
)

def parse_log(line):
    match = LOG_PATTERN.match(line)
    if not match:
        return None

    month, day, time, level, service, pid, message = (
        match.group(2),
        match.group(3),
        match.group(4),
        match.group(5),
        match.group(6),
        match.group(7),
        match.group(8)
    )

    # Convert date (Spark will use year=2025 as default)
    try:
        timestamp = datetime.strptime(f"{month} {day} {time}", "%b %d %H:%M:%S")
        timestamp = timestamp.replace(year=2025)
    except:
        timestamp = None

    return {
        "timestamp": str(timestamp),
        "level": level,
        "service": service,
        "pid": int(pid),
        "message": message.strip()
    }
