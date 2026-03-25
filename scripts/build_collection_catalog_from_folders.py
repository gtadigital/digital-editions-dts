import sys
import shutil
import click
from copy import deepcopy
from pathlib import Path
from typing import Any

from lxml import etree
from lxml.etree import Element, ElementTree
# debug run: uv run build_collection_catalog_from_folders.py ./sample_raw_data/semper ./out/catalog/semper-edition ./out/tei/semper-edition

from utils_xmltei_dts import parse_xml, write_xml_with_schema, append_collection_as_member_to_catalog

# module-level loggging
import logging
logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

NS = {"tei": "http://www.tei-c.org/ns/1.0"}

REFERENCES_DECLARATION = [
    # Logical: paragraph > lines
    etree.XML("""
        <refsDecl xmlns="http://www.tei-c.org/ns/1.0"
                  default="true"
                  n="logical_structure">
            <citeStructure unit="paragraph" match="//p" use="@xml:id">
                <citeStructure unit="line" match=".//lb" use="@n" delim="-"/>
            </citeStructure>
        </refsDecl>
    """),
    # Pages
    etree.XML("""
        <refsDecl xmlns="http://www.tei-c.org/ns/1.0"
                  n="published_page">
            <citeStructure unit="page" match="//pb" use="@xml:id" />
        </refsDecl>
    """)
]

# FUNCTIONS
# ---------
# main logic
def parse_teifolder_to_dapytains(
    source_tei_dir: Path,
    target_tei_dir: Path,
    target_catalog_dir: Path,
    catalog: ElementTree,
) -> Any: # Element
    """Convert a single TEI folder into dapytains catalog + tei output.

    Args:
        source_tei_dir: Directory containing TEI XML files (and optional schema files).
        target_tei_dir: Dapytains output root directory.
        catalog: Root catalog ElementTree.

    Returns:
        A ``<collection>`` Element to be appended as a member of the root catalog.

    Raises:
        ValueError: If *source_tei_dir* does not exist, is not a directory, contains
            no XML files, or if the catalog has no ``identifier`` attribute.
    """

    def extract_volume_titles(tree: ElementTree) -> list[str]:
        """Extract title strings from a TEI header.

        Args:
            tree: Parsed TEI ElementTree.

        Returns:
            List of title strings found (may be empty).
        """
        volume = tree.xpath(
            "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
            namespaces=NS,
        )
        monograph = tree.xpath(
            "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title[@level='m']",
            namespaces=NS,
        )
        titles = []
        if volume:
            titles.append(volume[0].text.strip())
        if monograph:
            titles.append(monograph[0].text.strip())
        return titles

    # --- Input validation ---
    if not source_tei_dir.exists():
        raise ValueError(f"source_tei_dir directory does not exist: {source_tei_dir}")
    if not source_tei_dir.is_dir():
        raise ValueError(f"source_tei_dir path is not a directory: {source_tei_dir}")
    xml_files = sorted(source_tei_dir.glob("*.xml"))
    if not xml_files:
        raise ValueError(f"No XML files found in directory: {source_tei_dir}")

    # (0) Setup # TODO: put in main, don't need to retrieve the id for each file --'
    catalog_root = catalog.getroot()
    base_identifier: str = catalog_root.get("identifier")
    if not base_identifier:
        raise ValueError("Root catalog does not have an 'identifier' attribute")

    # (1) ROOT CATALOG -A New Element pointing at Manuscript Catalog
    collection_xml_relative_path = f"./{source_tei_dir.name}.xml"
    collection_catalog = etree.Element(
        "collection",
        identifier=f"{base_identifier}/{source_tei_dir.name}",
        filepath=str(collection_xml_relative_path),
    )
    logger.debug(etree.tostring(collection_catalog, pretty_print=True, encoding="unicode"))

    # (2) MANUSCRIPT CATALOG - Build the "Manuscript as Collection" catalog
    #     with the facsimile pages as resources
    collection_manuscript = etree.Element(
        "collection",
        identifier=f"{base_identifier}/{source_tei_dir.name}",
    )
    title_elem = etree.SubElement(collection_manuscript, "title")
    first_tei_tree = parse_xml(xml_files[0])
    extracted_titles = first_tei_tree.xpath(
        "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
        namespaces=NS,
    )
    title_elem.text = (
        extracted_titles[0].text.strip() if extracted_titles else source_tei_dir.name
    )
    members_elem = etree.SubElement(collection_manuscript, "members")

    # (2.1) Iterate over the TEI source files to:
    # (2.1.a) add each one as member of the Manuscript Catalog,
    # (2.1.b) process the TEI File (inject refDesc for navigability, inject RNG schema) and copy to target_tei_dir
    for source_tei_file in xml_files:
        source_tei_tree = parse_xml(source_tei_file)
        titles = extract_volume_titles(source_tei_tree)
        logger.debug("  %s → titles: %s", source_tei_file.name, titles)

        resource_identifier = f"{base_identifier}/{source_tei_dir.name}/{source_tei_file.stem}"
        logger.debug("  resource identifier: %s", resource_identifier)

        resource_xml_path = Path(target_tei_dir, source_tei_dir.name, source_tei_file.name)
        resource_xml_relative_path = f"../../tei/semper-edition/{source_tei_dir.name}/{source_tei_file.name}" # relative to the collection catalog # TODO: remove this hardcoded horror, make a more broadly usable function that spawn a new collection
        resource = etree.SubElement(
            members_elem,
            "resource",
            identifier=resource_identifier,
            filepath=resource_xml_relative_path,
        )
        resource_title = etree.SubElement(resource, "title")
        tei_title_nodes = source_tei_tree.xpath(
            "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
            namespaces=NS,
        )
        resource_title.text = (
            "".join(tei_title_nodes[0].itertext())
            if tei_title_nodes
            else source_tei_file.stem
        )

        source_tei_tree_navigable = inject_citeStructure_in_refsDecl(
            tree=source_tei_tree, references_declaration=REFERENCES_DECLARATION
        )

        write_xml_with_schema(source_tei_tree_navigable, resource_xml_path)

    # (2.2) Write the collection manifest
    collection_target_tei_dir_path = Path(target_catalog_dir, f"{source_tei_dir.name}.xml")
    write_xml_with_schema(
        etree.ElementTree(collection_manuscript), collection_target_tei_dir_path
    )
    logger.debug("  written collection manifest → %s", collection_target_tei_dir_path)

    # (3) Copy local schema dependencies (.dtd, .rng)
    for f in source_tei_dir.glob("*"):
        if f.is_file() and f.suffix in {".dtd", ".rng"}:
            dst = Path(target_tei_dir, source_tei_dir.name, f.name)
            dst.parent.mkdir(exist_ok=True, parents=True)
            shutil.copy(src=f, dst=dst)
            logger.debug("  copied schema file %s → %s", f.name, dst)

    return collection_catalog

# make dts resources citable
def inject_citeStructure_in_refsDecl(
    tree: ElementTree, 
    references_declaration: list
) -> ElementTree:
    existing = tree.xpath("//tei:refsDecl", namespaces=NS)
    if existing:
        logger.info("   refsDecl already declared in TEI Header - skipping")
        return tree

    root = deepcopy(tree.getroot())
    new_tree = etree.ElementTree(root)

    tei_encDesc = new_tree.xpath(
        "/tei:TEI/tei:teiHeader/tei:encodingDesc", namespaces=NS
    )
    if not tei_encDesc:
        logger.warning(
            "    No encodingDesc found in TEI Header, this file won't be searchable"
        )
        return tree

    tei_encDesc = tei_encDesc[0]
    for ref in references_declaration:
        tei_encDesc.append(deepcopy(ref)) # lxml moves element before appending? so deepcopy to not working by reference

    return new_tree


# MAIN (Click CLI Entrypoint)
# --------------------------------------------------------------------------
@click.command()
@click.argument("source", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("target_catalog_dir", type=click.Path(file_okay=False, path_type=Path))
@click.argument("target_tei_dir", type=click.Path(file_okay=False, path_type=Path))
def main(source: Path, 
         target_catalog_dir: Path,
         target_tei_dir: Path
) -> None:
    # --- Validate `target_catalog_dir` catalog ---
    catalog_target_path = target_catalog_dir / "catalog.xml"
    if not catalog_target_path.exists():
        logger.error(
            "Catalog not found: %s .", catalog_target_path
        )
        sys.exit(1)

    logger.info("Loading catalog: %s", catalog_target_path)
    try:
        catalog_target: ElementTree = parse_xml(catalog_target_path)
    except Exception as exc:
        logger.error("Failed to parse root catalog %s: %s", catalog_target_path, exc)
        sys.exit(1)

    # --- Discover TEI folders at `source` ---
    source_tei_folders = sorted(p for p in source.iterdir() if p.is_dir())
    non_dirs = [p for p in source.iterdir() if not p.is_dir()]
    for p in non_dirs:
        logger.warning("Skipping non-directory entry in source: %s", p.name)

    if not source_tei_folders:
        logger.error("No sub-directories found in source: %s", source)
        sys.exit(1)

    logger.info("Found %d TEI folder(s) to process in %s", len(source_tei_folders), source)

    # --- Process each folder ---
    processed, failed = 0, 0
    for tei_folder in source_tei_folders:
        logger.info("Processing folder: %s", tei_folder.name)
        try:
            collection_elem = parse_teifolder_to_dapytains(
                source_tei_dir=tei_folder,
                target_tei_dir=target_tei_dir,
                target_catalog_dir=target_catalog_dir,
                catalog=catalog_target,
            )
            catalog_target = append_collection_as_member_to_catalog(
                catalog_tree=catalog_target,
                collection=collection_elem,
            )
            logger.info("  ✓ %s", tei_folder.name)
            processed += 1
        except Exception as exc:
            logger.error("  ✗ %s — %s", tei_folder.name, exc, exc_info=True)
            failed += 1
    
    # --- Write updated root catalog ---
    logger.info("Writing updated root catalog → %s", catalog_target_path)
    try:
        write_xml_with_schema(catalog_target, catalog_target_path)
    except Exception as exc:
        logger.error("Failed to write root catalog: %s", exc)
        sys.exit(1)

    # --- Summary ---
    logger.info(
        "Done — %d folder(s) processed successfully, %d failed.", processed, failed
    )
    if failed:
        sys.exit(2)

if __name__ == "__main__":
    main()