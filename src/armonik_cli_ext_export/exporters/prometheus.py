import os
import sys
import uuid
import tempfile
import tarfile
import subprocess
from typing import Optional
from armonik_cli_ext_export.utils import (
    console,
    get_aws_credentials,
    wait_for_job_completion,
)

import boto3
from kr8s.objects import Job, Pod
import rich_click as click
from rich.panel import Panel


def find_prometheus_pod(namespace: str) -> Pod:
    """Find the Prometheus pod in the specified namespace using kr8s"""
    try:
        pods = Pod.list(namespace=namespace)

        for pod in pods:
            if pod.name.startswith("prometheus"):
                return pod

        raise click.ClickException("No pod found starting with 'prometheus'")
    except Exception as e:
        console.print(f"[red]Error finding Prometheus pod: {e}[/red]")
        raise click.ClickException("Failed to find Prometheus pod")


def upload_to_s3(
    file_path: str,
    bucket_name: str,
    s3_key: str,
    aws_credentials: Optional[dict] = None,
):
    """Upload file to S3 using boto3"""
    try:
        # Create S3 client with credentials if provided
        if aws_credentials:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=aws_credentials["AWS_ACCESS_KEY_ID"],
                aws_secret_access_key=aws_credentials["AWS_SECRET_ACCESS_KEY"],
                aws_session_token=aws_credentials.get("AWS_SESSION_TOKEN", ""),
            )
        else:
            # Use default credentials
            s3_client = boto3.client("s3")

        # Upload file
        s3_client.upload_file(file_path, bucket_name, s3_key)
        console.print(
            f"[green]✓[/green] File uploaded to S3: [bold]s3://{bucket_name}/{s3_key}[/bold]"
        )

    except Exception as e:
        console.print(f"[red]✗ Error uploading to S3: {e}[/red]")
        raise click.ClickException(f"Failed to upload to S3: {e}")


def backup_local_mode(
    namespace: str,
    filename: str,
    bucket_name: str,
    aws_credentials: Optional[dict] = None,
):
    """Perform backup using local kubectl copy method"""
    console.print("Using Kubernetes copy method")

    # Find Prometheus pod using kr8s
    console.print("Finding Prometheus pod...")
    prometheus_pod = find_prometheus_pod(namespace)
    console.print(f"Found Prometheus pod: [bold cyan]{prometheus_pod.name}[/bold cyan]")

    with tempfile.TemporaryDirectory() as temp_dir:
        local_path = os.path.join(temp_dir, filename)
        tar_file_path = f"{local_path}.tar.gz"

        # Copy data from pod (using subprocess as kr8s doesn't support file copying)
        with console.status("Copying data from Prometheus pod..."):
            try:
                copy_cmd = [
                    "kubectl",
                    "cp",
                    f"{prometheus_pod.name}:/prometheus/",
                    local_path,
                    "-n",
                    namespace,
                ]
                subprocess.run(copy_cmd, check=True, capture_output=True)
            except subprocess.CalledProcessError as e:
                console.print(f"[red]✗ Error copying from pod: {e}[/red]")
                raise click.ClickException("Failed to copy file from pod")

        console.print("[green]✓[/green] Data directory copied from pod successfully")

        # Create tar file
        with console.status("Creating tar archive..."):
            try:
                with tarfile.open(tar_file_path, "w:gz") as tar:
                    tar.add(local_path, arcname=filename)
            except Exception as e:
                console.print(f"[red]✗ Error creating tar file: {e}[/]")
                raise click.ClickException("Failed to create tar file")

        console.print(f"[green]✓[/] Directory tarred successfully: [bold]{filename}.tar.gz[/]")

        # Upload to S3
        with console.status("Uploading to S3..."):
            s3_key = f"{filename}.tar.gz"
            upload_to_s3(tar_file_path, bucket_name, s3_key, aws_credentials)


def create_prometheus_backup_job(
    namespace: str,
    filename: str,
    bucket_name: str,
    aws_credentials: Optional[dict] = None,
) -> Job:
    """Create a Kubernetes Job to backup Prometheus data to S3"""
    # Generate a unique job name
    job_name = f"prom-s3-{str(uuid.uuid4())[:8]}"

    # Prepare AWS environment variables
    aws_env = []
    if aws_credentials:
        for key, value in aws_credentials.items():
            if value:  # Only add non-empty values
                aws_env.append({"name": key, "value": value})

    # Create the job spec
    job_spec = {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {"name": job_name, "namespace": namespace},
        "spec": {
            "template": {
                "spec": {
                    "containers": [
                        {
                            "name": "prom-snap",
                            "image": "richarvey/awscli:latest",
                            "env": aws_env,
                            "command": ["sh", "-c"],
                            "args": [
                                f"tar -czvf /tmp/{filename}.tar.gz /prometheus && aws s3 cp /tmp/{filename}.tar.gz s3://{bucket_name}/{filename}.tar.gz"
                            ],
                            "volumeMounts": [
                                {
                                    "name": "prometheus-volume",
                                    "mountPath": "/prometheus",
                                }
                            ],
                        }
                    ],
                    "restartPolicy": "Never",
                    "volumes": [
                        {
                            "name": "prometheus-volume",
                            "persistentVolumeClaim": {"claimName": "prometheus"},
                        }
                    ],
                    "ttlSecondsAfterFinished": 120,
                }
            },
            "backoffLimit": 4,
        },
    }

    # Create the job using kr8s
    try:
        job = Job(job_spec)
        job.create()
        console.print(
            f"[green]✓[/] Created Kubernetes Job '[bold cyan]{job_name}[/]' in namespace '[bold yellow]{namespace}[/]'"
        )
        return job
    except Exception as e:
        console.print(f"[red]✗ Error creating Kubernetes Job: {e}[/]")
        raise click.ClickException(f"Failed to create Kubernetes Job: {e}")


@click.command()
@click.option(
    "--namespace",
    default="armonik",
    help="Kubernetes namespace to use",
    show_default=True,
)
@click.option("--filename", required=True, help="Filename for the backup (without extension)")
@click.option("--s3-bucket", required=True, help="S3 bucket name to upload to")
@click.option("--aws-profile", help="AWS profile to use for S3 upload")
@click.option(
    "--local/--persistent-volume",
    default=False,
    help="Use local kubectl copy method instead of Persistent Volume",
)
@click.option(
    "--wait/--no-wait",
    default=False,
    help="Wait for the job to complete (only for persistent volume mode)",
)
@click.option(
    "--timeout",
    type=int,
    default=600,
    help="Timeout in seconds when waiting for job completion",
    show_default=True,
)
def prometheus_export_command(
    namespace: str,
    filename: str,
    s3_bucket: str,
    aws_profile: Optional[str],
    local: bool,
    wait: bool,
    timeout: int,
):
    """
    **Prometheus S3 Backup Tool**

    Backup Prometheus data to S3 using either local kubectl copy or Kubernetes Jobs.

    This tool provides two backup modes:
    - Local mode: Uses kubectl to copy data directly from Prometheus pod
    - Persistent Volume mode: Creates a Kubernetes Job to backup from PV

    **Examples:**

    Local backup:
    ```bash
    armonik export prometheus --filename prometheus-backup --s3-bucket my-s3-bucket --local
    ```

    Persistent Volume backup:
    ```bash
    armonik export prometheus --filename prometheus-backup --s3-bucket my-s3-bucket --aws-profile prod
    ```

    Backup and wait for completion:
    ```bash
    armonik export prometheus --filename backup --s3-bucket bucket --wait --timeout 1200
    ```
    """

    # Get AWS credentials if profile is provided
    aws_credentials = None
    if aws_profile:
        console.print(f"Getting AWS credentials from profile '[bold]{aws_profile}[/bold]'...")
        aws_credentials = get_aws_credentials(aws_profile)

        if not aws_credentials:
            console.print(
                f"[red]⚠[/] Could not get credentials from AWS profile '[bold]{aws_profile}[/bold]'"
            )
            if not click.confirm("Continue without AWS credentials?", default=False):
                raise click.Abort()
        else:
            console.print("[green]✓[/] AWS credentials loaded successfully")

    # Validate AWS credentials for non-local mode
    if not local and not aws_credentials:
        # Check if we have default AWS credentials
        try:
            boto3.Session().get_credentials()
            console.print("[green]✓[/] Using default AWS credentials")
        except Exception:
            console.print("[red]Error: AWS credentials are required for Persistent Volume mode[/]")
            console.print(
                "Either set environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY,"
            )
            console.print("provide an AWS profile with --aws-profile, or use --local mode")
            raise click.ClickException("Missing AWS credentials")

    # Display configuration
    mode_text = "Local (kubectl copy)" if local else "Persistent Volume (Kubernetes Job)"
    creds_text = (
        "Not required (local mode)"
        if local
        else ("Profile provided" if aws_profile else "Default credentials")
    )

    config_panel = Panel(
        f"""[bold]Backup Configuration:[/bold][]
        
• Namespace: [white]{namespace}[/]
• Mode: [white]{mode_text}[/]
• Filename: [white]{filename}[/]
• S3 Bucket: [white]{s3_bucket}[/]
• AWS Credentials: [white]{creds_text}[/]
• Wait for completion: [white]{"Yes" if wait and not local else "No" if local else "Yes" if wait else "No"}[/]
{f"• Timeout: [white]{timeout}s[/]" if wait and not local else ""}""",
        title="📋 Configuration",
        style="cyan",
    )
    console.print(config_panel)

    try:
        if local:
            console.print("\nStarting local backup...")
            backup_local_mode(namespace, filename, s3_bucket, aws_credentials)

            console.print(
                Panel(
                    f"[green]✅ Local backup completed successfully![/]\n\n"
                    f"Your Prometheus data has been backed up to: [bold]s3://{s3_bucket}/{filename}.tar.gz[/]",
                    title="🎉 Success",
                    style="green",
                )
            )
        else:
            console.print("\nCreating Prometheus backup job...")
            job = create_prometheus_backup_job(
                namespace=namespace,
                filename=filename,
                bucket_name=s3_bucket,
                aws_credentials=aws_credentials,
            )

            # Wait for job completion if requested
            if wait:
                console.print(f"\nMonitoring job completion (timeout: {timeout}s)...")
                success = wait_for_job_completion(job, timeout)

                if success:
                    console.print(
                        Panel(
                            f"[green]✅ Backup completed successfully![/]\n\n"
                            f"Your Prometheus data has been backed up to: [bold]s3://{s3_bucket}/{filename}.tar.gz[/]",
                            title="🎉 Success",
                            style="green",
                        )
                    )
                else:
                    console.print(
                        Panel(
                            f"[red]❌ Backup job failed or timed out[/]\n\n"
                            f"Check the Kubernetes job logs for more details:\n"
                            f"[bold]kubectl logs -n {namespace} job/{job.name}[/]",
                            title="💥 Failure",
                            style="red",
                        )
                    )
                    sys.exit(1)
            else:
                console.print(
                    Panel(
                        f"[yellow]🔄 Job created successfully![/]\n\n"
                        f"Monitor the job status with:\n"
                        f"[bold]kubectl get jobs -n {namespace} {job.name}[/]\n\n"
                        f"View job logs with:\n"
                        f"[bold]kubectl logs -n {namespace} job/{job.name}[/]",
                        title="📝 Next Steps",
                        style="yellow",
                    )
                )

    except Exception as e:
        console.print(Panel(f"[red]💥 An error occurred: {e}[/]", title="Error", style="red"))
        sys.exit(1)
