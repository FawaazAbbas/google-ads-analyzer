"""
Google Ads Analyzer Agent — CLI entry point
--------------------------------------------
Run: python main.py

Place your Google Ads CSV exports in the data/ folder, then run this script.
The agent will systematically analyze every dimension of your account and
produce a prioritized optimization report in the output/ folder.

For the web UI, run: streamlit run app.py
"""
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from agent.runner import run_analysis, EXPECTED_FILES

load_dotenv()
console = Console()


def detect_available_files(data_dir="data"):
    available = []
    for f in EXPECTED_FILES:
        if Path("{}/{}".format(data_dir, f)).exists():
            available.append(f)
    return available


def show_welcome(available_files):
    console.print()
    console.print(Panel.fit(
        "[bold cyan]Google Ads Analyzer Agent[/bold cyan]\n"
        "[dim]Powered by Claude claude-opus-4-6[/dim]",
        border_style="cyan"
    ))
    console.print()

    table = Table(title="Data Files Detected", show_header=True, header_style="bold")
    table.add_column("File", style="cyan")
    table.add_column("Status")

    for f in EXPECTED_FILES:
        if f in available_files:
            table.add_row(f, "[green]✓ Found[/green]")
        else:
            table.add_row(f, "[dim red]✗ Missing (will skip)[/dim red]")

    console.print(table)
    console.print()

    if not available_files:
        console.print("[bold red]No CSV files found in data/ folder.[/bold red]")
        console.print("Place your Google Ads CSV exports in the data/ folder and try again.")
        console.print("[dim]Or run the web UI with: streamlit run app.py[/dim]")
        sys.exit(1)


def run_agent(available_files):
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        console.print("[bold red]Error: ANTHROPIC_API_KEY not found.[/bold red]")
        console.print("Create a .env file with: ANTHROPIC_API_KEY=sk-ant-...")
        sys.exit(1)

    console.print("[bold yellow]Starting analysis...[/bold yellow]")
    console.print("[dim]Claude will call each analysis tool, then synthesize all findings.[/dim]")
    console.print()

    tool_call_count = 0

    for event_type, data in run_analysis(available_files, api_key):

        if event_type == "status":
            console.print("[dim]{}[/dim]".format(data))

        elif event_type == "thinking":
            console.print("[bold cyan]Claude is thinking...[/bold cyan]")

        elif event_type == "tool_start":
            console.print("  [cyan]->[/cyan] Running [bold]{}[/bold]...".format(data), end=" ")

        elif event_type == "tool_done":
            tool_call_count += 1
            console.print("[green]done[/green]")

        elif event_type == "complete":
            console.print()
            console.print(
                "[bold green]Analysis complete![/bold green] "
                "Called {} tools.".format(tool_call_count)
            )
            console.print()
            console.print(Panel(
                data[:3000] + (
                    "\n\n[dim]... (see full report in output/ folder)[/dim]"
                    if len(data) > 3000 else ""
                ),
                title="[bold]Optimization Report Preview[/bold]",
                border_style="blue",
            ))

        elif event_type == "error":
            console.print("[bold red]Error: {}[/bold red]".format(data))
            sys.exit(1)


def main():
    available_files = detect_available_files()
    show_welcome(available_files)
    run_agent(available_files)


if __name__ == "__main__":
    main()
