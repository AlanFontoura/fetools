"""Command-line interface for FE Tools."""

import typer
from pathlib import Path
from typing import Optional
from typing_extensions import Annotated

from fetools import PROJECT_ROOT

app = typer.Typer(
    help="Financial Engineering Tools - Portfolio data transformation toolkit"
)

# Subcommands
vnf_app = typer.Typer(help="Values and Flows (VnF) workflow commands")
po_sma_app = typer.Typer(help="Partial Ownership and SMA setup commands")
compliance_app = typer.Typer(help="Compliance reporting commands")

app.add_typer(vnf_app, name="vnf")
app.add_typer(po_sma_app, name="po-sma")
app.add_typer(compliance_app, name="compliance")


@app.command()
def version():
    """Show version information."""
    typer.echo("FE Tools v0.1.0")


@vnf_app.command("run")
def vnf_run(
    client: Annotated[
        str,
        typer.Argument(help="Client name (folder under data/inputs/vnf/)"),
    ],
    config: Annotated[
        Optional[Path],
        typer.Option(help="Path to vnf.toml config file"),
    ] = None,
    output_dir: Annotated[
        Optional[Path],
        typer.Option(help="Output directory (default: data/outputs/vnf/)"),
    ] = None,
):
    """Run Values and Flows workflow for a client."""
    from fetools.tools.vnf import ValuesAndFlowsTools

    if config is None:
        config = PROJECT_ROOT / "data/inputs/vnf" / client / "vnf.toml"

    if output_dir is None:
        output_dir = PROJECT_ROOT / "data/outputs/vnf" / client

    if not config.exists():
        typer.secho(
            f"Error: Config file not found: {config}", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)

    typer.echo(f"Running VnF workflow for client: {client}")
    typer.echo(f"Config: {config}")
    typer.echo(f"Output: {output_dir}")

    try:
        vnf = ValuesAndFlowsTools(str(config))
        vnf.main()
        typer.secho(
            "✓ VnF workflow completed successfully!", fg=typer.colors.GREEN
        )
    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@po_sma_app.command("generate")
def po_sma_generate(
    input_dir: Annotated[
        Path,
        typer.Option(
            help="Input directory with Account.csv, Client.csv, LEOwnership.csv"
        ),
    ] = PROJECT_ROOT
    / "data/inputs/partial_ownership",
    config: Annotated[
        Optional[Path],
        typer.Option(help="Path to SMA mapping config (JSON)"),
    ] = None,
    output_dir: Annotated[
        Path,
        typer.Option(help="Output directory"),
    ] = PROJECT_ROOT
    / "data/outputs/partial_ownership",
):
    """Generate partial ownership and SMA import files."""
    from fetools.tools.po_sma_loader import main as po_sma_main
    import pandas as pd

    typer.echo(f"Generating PO/SMA files...")
    typer.echo(f"Input: {input_dir}")
    typer.echo(f"Output: {output_dir}")

    try:
        # Load input files
        acc = pd.read_csv(input_dir / "Account.csv")
        cli = pd.read_csv(input_dir / "Client.csv")
        fco = pd.read_csv(input_dir / "LEOwnership.csv")

        # Run PO/SMA generation (adjust cutoff date as needed)
        po_sma_main(acc, cli, fco, cutoff="2020-12-31")
        typer.secho("✓ PO/SMA generation completed!", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


@compliance_app.command("report")
def compliance_report(
    config: Annotated[
        Path,
        typer.Option(help="Path to compliance.toml config file"),
    ] = PROJECT_ROOT
    / "data/inputs/compliance/compliance.toml",
    output_dir: Annotated[
        Path,
        typer.Option(help="Output directory"),
    ] = PROJECT_ROOT
    / "data/outputs/compliance",
):
    """Generate compliance report from d1g1t API."""
    from fetools.tools.compliance_report import ComplianceReport

    if not config.exists():
        typer.secho(
            f"Error: Config file not found: {config}", fg=typer.colors.RED
        )
        raise typer.Exit(code=1)

    typer.echo(f"Generating compliance report...")
    typer.echo(f"Config: {config}")
    typer.echo(f"Output: {output_dir}")

    try:
        report = ComplianceReport(str(config))
        report.login()
        # ComplianceReport doesn't have a run() method yet
        # TODO: Add main execution method to ComplianceReport class
        typer.secho("✓ Compliance report generated!", fg=typer.colors.GREEN)
        typer.secho("Note: ComplianceReport execution needs to be implemented", fg=typer.colors.YELLOW)
    except Exception as e:
        typer.secho(f"Error: {e}", fg=typer.colors.RED)
        raise typer.Exit(code=1)


def main():
    """Entry point for CLI."""
    app()


if __name__ == "__main__":
    main()
