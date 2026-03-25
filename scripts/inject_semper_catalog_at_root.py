import sys
import click
from copy import deepcopy
from pathlib import Path

from lxml import etree
from lxml.etree import Element, ElementTree

from utils_xmltei_dts import parse_xml, write_xml_with_schema, append_collection_as_member_to_catalog


# module-level loggging
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
# debug run: uv run inject_semper_catalog_at_root.py ./out


# MAIN (Click CLI Entrypoint)
# --------------------------------------------------------------------------
@click.command()
@click.argument("target", type=click.Path(file_okay=False, path_type=Path))
def main(
    target: Path
) -> None:
    # catalog path (path dts style)
    catalog_master_path = target / "catalog" / "catalog.xml"

    # --- Validate target catalog ---
    # catalog.xml exists
    if not catalog_master_path.exists():
        logger.error(
            "catalog.xml does not exist at target," \
            "the target DTS Data folder shall be initialized first with catalog.xml, catalog and tei folders.",
            catalog_master_path,
        )
        sys.exit(1)
    
    # it loads
    logger.info("Loading root catalog: %s", catalog_master_path)
    try:
        catalog_master: ElementTree = parse_xml(catalog_master_path)
    except Exception as exc:
        logger.error("Failed to parse root catalog %s: %s", catalog_master_path, exc)
        sys.exit(1)

    # it have an identifier
    catalog_master_root: Element = catalog_master.getroot()
    base_identifier: str = catalog_master_root.get("identifier")
    if not base_identifier:
        raise ValueError("Root catalog does not have an 'identifier' attribute")


    # --- STATICS ---
    COLLECTION_SEMPER_ID = f"{base_identifier}/semper-edition"
    CATALOG_SEMPER_FULLPATH = target / "catalog" / "semper-edition" / "catalog.xml"
    CATALOG_SEMPER_RELATIVE_PATH = "semper-edition/catalog.xml" # relative to the master catalog location
    CATALOG_SEMPER_HEADER: str = f"""<?xml version='1.0' encoding='UTF-8'?>
    <?oxygen RNGSchema="schema.rng" type="xml"?>
    <collection identifier="{COLLECTION_SEMPER_ID}">
        <title>Digital Semper Edition</title>
        <dublinCore>
            <creator>SNF Semper, Mendrisio, GTA Digital...</creator>
            <!-- <contributor>e.g. Editor or Translator Name</contributor> -->
            <description>Semper-Editions collection</description>
            <!-- <date>YYYY or YYYY-MM-DD</date> -->
            <language>de</language>
            <!-- <publisher>Institution or publisher name</publisher> -->
            <!-- <rights>License or rights statement, e.g. CC-BY 4.0</rights> -->
            <subject>Critical Editions, Gottfried Semper</subject>
            <!-- <coverage>Temporal or spatial scope of the material</coverage> -->
            <!-- <type>DTS resource type, e.g. Collection</type> -->
            <!-- <source>Original source or provenance</source> -->
            <format>application/tei+xml</format>
        </dublinCore>
        <!-- Add members -->
    </collection>
    """


    # --- First, Inject semper-edition catalog as target (root) catalog member (STATIC, HARDCODE) --
    logger.info(f"Injecting semper-edition collection at \n\t+ target='{catalog_master_path}'\n\t+ base_identifier='{base_identifier}'")
    cm = etree.Element(
        "collection",
        identifier=COLLECTION_SEMPER_ID,
        filepath=str(CATALOG_SEMPER_RELATIVE_PATH),
    )
    logger.debug(etree.tostring(cm, pretty_print=True, encoding="unicode"))

    try:
        catalog_master_root_updated = append_collection_as_member_to_catalog(catalog_tree=catalog_master, collection=cm)
        write_xml_with_schema(
            tree=catalog_master_root_updated, output_path=catalog_master_path
        )
    except Exception as exc:
        logger.error("Failed to write catalog.xml: %s", exc)
        sys.exit(1)

    # --- Then, write the semper-editions catalog header (even more STATIC and HARDCODED) --
    try:
        # just parse the CATALOG_SEMPER_HEADER and write out to its destination
        catalog_semper: ElementTree = etree.ElementTree(
            etree.fromstring(CATALOG_SEMPER_HEADER.encode("utf-8"))
        )
        write_xml_with_schema(
            tree=catalog_semper, output_path=CATALOG_SEMPER_FULLPATH
        )
    except Exception as exc:
        logger.error("Failed to write catalog.xml: %s", exc)
        sys.exit(1)



    logger.info("Ok, target=%s", target)

if __name__ == "__main__":
    main()
