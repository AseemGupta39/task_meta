# import logging

# logging.basicConfig(level=logging.INFO) 
# logger = logging.getLogger("file_processor")

import sys
import datetime

class TerminalLogger:
    LEVELS = {
        "DEBUG": "\033[94m",    # Blue
        "INFO": "\033[92m",     # Green
        "WARNING": "\033[93m",  # Yellow
        "ERROR": "\033[91m",    # Red
        "ENDC": "\033[0m"       # Reset
    }

    def __init__(self, name="Logger"):
        self.name = name

    def _log(self, level, message):
        timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        color = self.LEVELS.get(level, "")
        endc = self.LEVELS["ENDC"]
        formatted = f"{color}[{timestamp}] [{self.name}] [{level}] {message}{endc}"
        print(formatted, file=sys.stdout if level != "ERROR" else sys.stderr)

    def debug(self, message): self._log("DEBUG", message)
    def info(self, message): self._log("INFO", message)
    def warning(self, message): self._log("WARNING", message)
    def error(self, message): self._log("ERROR", message)

    def easyPrint(self, message): self.info(f"\n\n\n\n{message}\n\n\n\n")


logger = TerminalLogger("MyApp")