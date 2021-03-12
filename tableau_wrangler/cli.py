"""Console script for tableau_wrangler."""
import sys
import click


@click.command()
def main(args=None):
    """Console script for tableau_wrangler."""
    click.echo("Replace this message by putting your code into "
               "tableau_wrangler.cli.main")
    click.echo("See click documentation at https://click.palletsprojects.com/")
    return 0


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
