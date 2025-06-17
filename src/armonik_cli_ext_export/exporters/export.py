import rich_click as click
from .mongodb import mongodb_export_command
from .prometheus import prometheus_export_command


@click.group(name="export")
def export_group():
    """Export various resources out of ArmoniK."""
    pass


export_group.add_command(mongodb_export_command, name="mongodb")
export_group.add_command(prometheus_export_command, name="prometheus")
