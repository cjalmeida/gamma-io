from pathlib import Path
from gamma.config.scaffold import get_source
from gamma.dispatch import dispatch


class GammaIOScaffold:
    name = "gamma-io"


def setup():
    return GammaIOScaffold()


@dispatch
def get_source(mod: GammaIOScaffold):
    return Path(__file__).parent / "scaffold_sample"
