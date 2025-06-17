import sys
import uuid
from datetime import datetime
from typing import Optional

from armonik_cli_ext_export.utils import (
    console,
    get_aws_credentials,
    wait_for_job_completion,
)

from kr8s.objects import Job
import rich_click as click
from rich.panel import Panel


def create_mongodb_export_job(
    namespace: str,
    collection_name: str,
    s3_bucket: str,
    s3_key: str,
    aws_credentials: Optional[dict] = None,
    mongodb_secret: str = "mongodb",
) -> Job:
    """Create a Kubernetes Job to export MongoDB data to S3"""
    # Generate a unique job name
    job_name = f"mongo-export-{collection_name.lower()}-{str(uuid.uuid4())[:8]}"

    # Prepare AWS environment variables
    aws_env = []
    if aws_credentials:
        for key, value in aws_credentials.items():
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
                            "name": "sling",
                            "image": "slingdata/sling",
                            "command": ["/bin/sh", "-c"],
                            "args": [
                                f"""
                            # Use the environment variables directly
                            export MONGODB="mongodb://$MONGO_USER:$MONGO_PASS@$MONGO_HOST:$MONGO_PORT/database?ssl=true&tlsInsecure=true"
                            # Run the Sling command
                            sling run --src-conn MONGODB --src-stream 'database.{collection_name}' --tgt-conn S3 --tgt-object "s3://{s3_bucket}/{s3_key}"
                            """
                            ],
                            "env": [
                                {
                                    "name": "MONGO_USER",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": mongodb_secret,
                                            "key": "username",
                                        }
                                    },
                                },
                                {
                                    "name": "MONGO_PASS",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": mongodb_secret,
                                            "key": "password",
                                        }
                                    },
                                },
                                {
                                    "name": "MONGO_HOST",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": mongodb_secret,
                                            "key": "host",
                                        }
                                    },
                                },
                                {
                                    "name": "MONGO_PORT",
                                    "valueFrom": {
                                        "secretKeyRef": {
                                            "name": mongodb_secret,
                                            "key": "port",
                                        }
                                    },
                                },
                            ]
                            + aws_env,
                            "volumeMounts": [
                                {
                                    "name": "mongodb-cert",
                                    "mountPath": "/mongodb/certs",
                                    "readOnly": True,
                                }
                            ],
                        }
                    ],
                    "restartPolicy": "Never",
                    "volumes": [
                        {
                            "name": "mongodb-cert",
                            "secret": {
                                "secretName": mongodb_secret,
                                "items": [{"key": "chain.pem", "path": "chain.pem"}],
                            },
                        }
                    ],
                }
            },
            "backoffLimit": 4,
        },
    }

    # Create the job
    try:
        job = Job(job_spec)
        job.create()
        console.print(
            f"[green]✓[/green] Created Kubernetes Job '[bold cyan]{job_name}[/bold cyan]' in namespace '[bold yellow]{namespace}[/bold yellow]'"
        )
        return job
    except Exception as e:
        console.print(f"[red]✗ Error creating Kubernetes Job: {e}[/red]")
        raise click.ClickException(f"Failed to create Kubernetes Job: {e}")


@click.command()
@click.option(
    "--namespace",
    default="armonik",
    help="Kubernetes namespace to use",
    show_default=True,
)
@click.option(
    "--mongodb-secret",
    default="mongodb",
    help="Name of the Kubernetes secret containing MongoDB credentials",
    show_default=True,
)
@click.option(
    "--collection",
    default="TaskData",
    help="Collection name to backup",
    show_default=True,
)
@click.option("--s3-bucket", required=True, help="S3 bucket name to upload to")
@click.option("--s3-key", help="S3 object key/path (auto-generated if not provided)")
@click.option("--aws-profile", help="AWS profile to use for S3 upload")
@click.option("--wait/--no-wait", default=False, help="Wait for the job to complete")
@click.option(
    "--timeout",
    type=int,
    default=600,
    help="Timeout in seconds when waiting for job completion",
    show_default=True,
)
def mongodb_export_command(
    namespace: str,
    mongodb_secret: str,
    collection: str,
    s3_bucket: str,
    s3_key: Optional[str],
    aws_profile: Optional[str],
    wait: bool,
    timeout: int,
):
    """
    **MongoDB to S3 Export Tool**

    Export MongoDB collections to S3 using Kubernetes Jobs with the Sling data tool.

    This tool creates a Kubernetes Job that uses Sling to export data from a MongoDB
    collection directly to an S3 bucket.

    **Examples:**

    Basic export:
    ```bash
    armonik export mongodb --s3-bucket my-backup-bucket
    ```

    Export specific collection with custom S3 path:
    ```bash
    armonik export mongodb --collection Users --s3-bucket my-bucket --s3-key exports/users/backup.json
    ```

    Export and wait for completion:
    ```bash
    armonik export mongodb --s3-bucket my-bucket --wait --timeout 1200
    ```
    """

    # Show a nice header

    # Generate S3 key if not provided
    if not s3_key:
        timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
        s3_key = f"exports/{collection}/{timestamp}.json"
        console.print(f"Auto-generated S3 key: [bold]{s3_key}[/]")

    # Get AWS credentials if profile is provided
    aws_credentials = None
    if aws_profile:
        console.print(f"Getting AWS credentials from profile '[bold]{aws_profile}[/]'...")
        aws_credentials = get_aws_credentials(aws_profile)

        if not aws_credentials:
            console.print(
                f"[red]⚠[/] Could not get credentials from AWS profile '[bold]{aws_profile}[/]'"
            )
            raise click.Abort()
        else:
            console.print("[green]✓[/] AWS credentials loaded successfully")

    # Display export configuration
    config_panel = Panel(
        f"""[bold]Export Configuration:[/]
        
• Namespace: [white]{namespace}[/]
• Collection: [white]{collection}[/]
• S3 Bucket: [white]{s3_bucket}[/]
• S3 Key: [white]{s3_key}[/]
• MongoDB Secret: [white]{mongodb_secret}[/]
• AWS Profile: [white]{aws_profile or "None (using default credentials)"}[/]
• Wait for completion: [white]{"Yes" if wait else "No"}[/]
{f"• Timeout: {timeout}s" if wait else ""}""",
        title="📋 Configuration",
        style="cyan",
    )
    console.print(config_panel)

    # Create the export job
    console.print("\nCreating MongoDB export job...")

    try:
        job = create_mongodb_export_job(
            namespace=namespace,
            collection_name=collection,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            aws_credentials=aws_credentials,
            mongodb_secret=mongodb_secret,
        )

        # Wait for job completion if requested
        if wait:
            console.print(f"\n[blue]⏳[/] Monitoring job completion (timeout: {timeout}s)...")
            success = wait_for_job_completion(job, timeout)

            if success:
                console.print(
                    Panel(
                        f"[green]✅ Export completed successfully![/]\n\n"
                        f"Your data has been exported to: [bold]s3://{s3_bucket}/{s3_key}[/]",
                        title="🎉 Success",
                        style="green",
                    )
                )
            else:
                console.print(
                    Panel(
                        f"[red]❌ Export job failed or timed out[/]\n\n"
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
