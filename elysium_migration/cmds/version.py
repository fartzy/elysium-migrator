import click

version = "0.1.0"


def print_version_callback(ctx, param, value):

    if not value or ctx.resilient_parsing:
        return
    click.echo("Version {}".format(version))
    ctx.exit()
