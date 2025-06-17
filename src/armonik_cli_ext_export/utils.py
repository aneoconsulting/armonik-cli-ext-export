from typing import Optional
import boto3

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, TimeElapsedColumn
from kr8s.objects import Job

import time

console = Console()


def get_aws_credentials(profile_name: Optional[str] = None) -> Optional[dict]:
    """Get AWS credentials from the specified profile"""
    if profile_name:
        try:
            session = boto3.Session(profile_name=profile_name)
            credentials = session.get_credentials()
            if credentials:
                frozen_credentials = credentials.get_frozen_credentials()
                return {
                    "AWS_ACCESS_KEY_ID": frozen_credentials.access_key,
                    "AWS_SECRET_ACCESS_KEY": frozen_credentials.secret_key,
                    "AWS_SESSION_TOKEN": frozen_credentials.token
                    if frozen_credentials.token
                    else "",
                }
        except Exception as e:
            console.print(f"[red]Error getting AWS credentials: {e}[/red]")
    return None


def wait_for_job_completion(job: Job, timeout_seconds: int = 600) -> bool:
    """Wait for the job to complete and return its status"""

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        TimeElapsedColumn(),
        console=console,
        transient=True,
    ) as progress:
        task = progress.add_task(f"Waiting for job {job.name} to complete...", total=None)
        start_time = time.time()

        while True:
            job.refresh()

            if "conditions" in job.status and job.status.get("conditions"):
                for condition in job.status["conditions"]:
                    if condition["type"] == "Complete" and condition["status"] == "True":
                        progress.stop()
                        console.print(f"[green]✓ Job {job.name} completed successfully[/green]")
                        return True
                    elif condition["type"] == "Failed" and condition["status"] == "True":
                        progress.stop()
                        console.print(
                            f"[red]✗ Job {job.name} failed: {condition.get('message', 'Unknown error')}[/red]"
                        )
                        return False

            # Check for timeout
            elapsed = time.time() - start_time
            if elapsed > timeout_seconds:
                progress.stop()
                console.print(f"[red]✗ Timeout waiting for job {job.name} to complete[/red]")
                return False

            # Update progress description with elapsed time
            progress.update(
                task,
                description=f"Waiting for job {job.name} to complete... ({elapsed:.0f}s/{timeout_seconds}s)",
            )
            time.sleep(5)
