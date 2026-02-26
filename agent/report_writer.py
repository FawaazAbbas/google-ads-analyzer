"""
Saves the final optimization report to the output/ directory as a Markdown file.
"""
import os
from datetime import datetime


def save_report(content: str, output_dir: str = "output") -> str:
    """
    Save the report markdown content to output/report_YYYYMMDD_HHMMSS.md
    Returns the file path.
    """
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"report_{timestamp}.md"
    filepath = os.path.join(output_dir, filename)

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(content)

    return filepath
