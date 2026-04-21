import logging
import sys
from copy import deepcopy
from pathlib import Path

import click    # already in dapytains requirements :emoji_fire:
from lxml import etree
from lxml.etree import ElementTree

logger = logging.getLogger(__name__)


# Dublin Core fields for Collection (or Resource)
# -----------------------------------------------
_DUBLIN_CORE_FIELDS: list[tuple[str, str]] = [
    ("creator", "e.g. Author Name"),
    ("contributor", "e.g. Editor or Translator Name"),
    ("description", "Short description of the collection"),
    ("date", "YYYY or YYYY-MM-DD"),
    ("language", "ISO 639-1 code, e.g. en, de, fr, la"),
    ("publisher", "Institution or publisher name"),
    ("rights", "License or rights statement, e.g. CC-BY 4.0"),
    ("subject", "Keyword or controlled-vocabulary term"),
    ("coverage", "Temporal or spatial scope of the material"),
    ("type", "DTS resource type, e.g. Collection"),
    ("source", "Original source or provenance"),
    ("format", "Media type, e.g. application/tei+xml"),
]


# Functions
# ---------
def build_root_catalog_xml(identifier: str, title: str) -> ElementTree:
    """Build a skeleton dapytains root catalog ElementTree.

    Args:
        identifier: Unique URI for the root DTS collection
        title: Title for the collection.

    Returns:
        catalog as a lxml ElementTree
    """
    root = etree.Element("collection", identifier=identifier)

    title_elem = etree.SubElement(root, "title")
    title_elem.text = title

    dublin = etree.SubElement(root, "dublinCore")
    for field_name, hint in _DUBLIN_CORE_FIELDS:
        dublin.append(etree.Comment(f" <{field_name}>{hint}</{field_name}> "))
    return etree.ElementTree(root)

def write_catalog(tree: ElementTree, output_path: Path) -> None:
    """Write a catalog ElementTree to *output_path* with an oXygen RNG PI.

    Args:
        tree: The catalog ElementTree to serialise.
        output_path: Destination file path.
    """
    root = deepcopy(tree.getroot())
    pi = etree.ProcessingInstruction("oxygen", 'RNGSchema="schema.rng" type="xml"')
    root.addprevious(pi)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    etree.ElementTree(root).write(
        output_path,
        pretty_print=True,
        xml_declaration=True,
        encoding="UTF-8",
    )
    logger.debug("Written catalog → %s", output_path)


# MAIN (Click CLI Entrypoint)
# --------------------------------------------------------------------------
@click.command()
@click.argument("target", type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--identifier",
    required=True,
    help="Unique URI for the root DTS collection",
)
@click.option(
    "--title",
    required=True,
    help="Title for the collection.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    help="Overwrite an existing catalog.xml if present.",
)
@click.option(
    "--log-level",
    default="INFO",
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Logging verbosity.",
)
def main(
    target: Path, identifier: str, title: str, overwrite: bool, log_level: str
) -> None:
    """Initialise a dapytains output directory at TARGET.
    Creates the ``catalog/`` and ``tei/`` sub-directories and writes a
    skeleton ``catalog/catalog.xml`` with Dublin Core placeholders.

    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    catalog_path = target / "catalog" / "catalog.xml"
    tei_path = target / "tei"

    # --- Guard against accidental overwrite ---
    if catalog_path.exists() and not overwrite:
        logger.error(
            "catalog.xml already exists at %s. Pass --overwrite to replace it.",
            catalog_path,
        )
        sys.exit(1)

    # --- Create directory structure ---
    for directory in (target / "catalog", tei_path):
        directory.mkdir(parents=True, exist_ok=True)
        logger.debug("Created directory: %s", directory)

    # --- Build and write catalog ---
    logger.info("Initialising catalog with identifier: %s", identifier)
    try:
        catalog_tree = build_root_catalog_xml(identifier=identifier, title=title)
        write_catalog(catalog_tree, catalog_path)
    except Exception as exc:
        logger.error("Failed to write catalog.xml: %s", exc)
        sys.exit(1)

    logger.info("Ok, target=%s", target)


if __name__ == "__main__":
    main()
