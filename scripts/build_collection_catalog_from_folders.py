"""
build_collection_catalog_from_folders.py
====================================
CLI script to batch-convert a collection of **FLAT** TEI folders into a
dapytains-compatible catalog/tei output structure.

Expected source layout::

    source/
        <folder-manuscript-A>/
            manuscript-element-1.xml
            manuscript-element-2.xml
            schema.rng
            chars.dtd
        <folder-manuscript-B>/
            ...

Expected target initial layout::

    target/
        catalog/
            catalog.xml   ← root catalog, will be updated in-place
        tei/

Expected target final layout::
    target/
        catalog/
            catalog.xml              ← updated with new <collection> members
            collection-<folder-manuscript-A>.xml
            collection-<folder-manuscript-B>.xml
        tei/
            <folder-manuscript-A>/
                manuscript-element-1.xml
                manuscript-element-2.xml
                ...
        
"""

import shutil
import sys
from copy import deepcopy
from pathlib import Path

import click
from lxml import etree
from lxml.etree import Element, ElementTree

# Static
# ------
NS = {"tei": "http://www.tei-c.org/ns/1.0"}

REFERENCES_DECLARATION = [
    # Logical
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

# Logging
import logging

logger = logging.getLogger(__name__)


# FUNCTIONS
# ---------
# Collections
# -----------
def parse_teifolder_to_dapytains(
    source: Path,
    target: Path,
    catalog: ElementTree,
) -> Element:
    """Convert a single TEI folder into dapytains catalog + tei output.

    Args:
        source: Directory containing TEI XML files (and optional schema files).
        target: Dapytains output root directory.
        catalog: Root catalog ElementTree.

    Returns:
        A ``<collection>`` Element to be appended as a member of the root catalog.

    Raises:
        ValueError: If *source* does not exist, is not a directory, contains
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
    if not source.exists():
        raise ValueError(f"Source directory does not exist: {source}")
    if not source.is_dir():
        raise ValueError(f"Source path is not a directory: {source}")
    xml_files = sorted(source.glob("*.xml"))
    if not xml_files:
        raise ValueError(f"No XML files found in directory: {source}")

    # (0) Setup
    catalog_root = catalog.getroot()
    base_identifier: str = catalog_root.get("identifier")
    if not base_identifier:
        raise ValueError("Root catalog does not have an 'identifier' attribute")

    # (1) ROOT CATALOG -A New Element pointing at Manuscript Catalog
    collection_xml_relative_path = f"./collection-{source.name}.xml"
    collection_catalog = etree.Element(
        "collection",
        identifier=f"{base_identifier}/{source.name}",
        filepath=str(collection_xml_relative_path),
    )

    # (2) MANUSCRIPT CATALOG - Build the "Manuscript as Collection" catalog
    #     with the facsimile pages as resources
    collection_manuscript = etree.Element(
        "collection",
        identifier=f"{base_identifier}/{source.name}",
    )
    title_elem = etree.SubElement(collection_manuscript, "title")
    first_tei_tree = parse_xml(xml_files[0])
    extracted_titles = first_tei_tree.xpath(
        "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
        namespaces=NS,
    )
    title_elem.text = (
        extracted_titles[0].text.strip() if extracted_titles else source.name
    )
    members_elem = etree.SubElement(collection_manuscript, "members")

    # (2.1) Iterate over the TEI source files to:
    # (2.1.a) add each one as member of the Manuscript Catalog,
    # (2.1.b) process the TEI File (inject refDesc for navigability, inject RNG schema) and copy to target
    for source_tei_file in xml_files:
        source_tei_tree = parse_xml(source_tei_file)
        titles = extract_volume_titles(source_tei_tree)
        logger.debug("  %s → titles: %s", source_tei_file.name, titles)

        resource_identifier = f"{base_identifier}/{source.name}/{source_tei_file.stem}"
        logger.debug("  resource identifier: %s", resource_identifier)

        resource_xml_path = Path(target, "tei", source.name, source_tei_file.name)
        resource_xml_relative_path = f"../tei/{source.name}/{source_tei_file.name}"
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
    collection_target_path = Path(target, "catalog", f"collection-{source.name}.xml")
    write_xml_with_schema(
        etree.ElementTree(collection_manuscript), collection_target_path
    )
    logger.debug("  written collection manifest → %s", collection_target_path)

    # (3) Copy local schema dependencies (.dtd, .rng)
    for f in source.glob("*"):
        if f.is_file() and f.suffix in {".dtd", ".rng"}:
            dst = Path(target, "tei", source.name, f.name)
            dst.parent.mkdir(exist_ok=True, parents=True)
            shutil.copy(src=f, dst=dst)
            logger.debug("  copied schema file %s → %s", f.name, dst)

    return collection_catalog


def append_collection_as_member_to_catalog(
    catalog_tree: ElementTree,
    collection: Element,
) -> ElementTree:
    """Append a collection element as a member of an existing catalog tree.

    Args:
        catalog_tree: The parent catalog ElementTree to update.
        collection: The ``<collection>`` Element to append.

    Returns:
        A new catalog ElementTree with the collection appended.
    """
    root = deepcopy(catalog_tree.getroot())
    members_element = root.find("members")
    if members_element is None:
        members_element = etree.SubElement(root, "members")
    members_element.append(collection)
    return etree.ElementTree(root)


# Resources
# -------
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


# Helpers
# -------
def parse_xml(path: Path | str) -> ElementTree:
    """Parse an XML file and return the lxml ElementTree. With DTD

    Args:
        path: Filesystem path to the XML file.

    Returns:
        Parsed lxml ElementTree.
    """
    parser = etree.XMLParser(remove_blank_text=True, load_dtd=True)
    return etree.parse(str(path), parser)


def write_xml_with_schema(tree: ElementTree, output_path: Path) -> None:
    """Serialise an ElementTree with schema RNG instructions

    Args:
        tree: The ElementTree to write.
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


# ----
# MAIN
# ----
@click.command()
@click.argument("source", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.argument("target", type=click.Path(file_okay=False, path_type=Path))
@click.option(
    "--log-level",
    default="INFO",
    show_default=True,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"], case_sensitive=False),
    help="Logging verbosity.",
)
def main(source: Path, target: Path, log_level: str) -> None:
    """Parse all TEI folders in SOURCE and write dapytains output to TARGET.

    SOURCE must be a directory, containing sub-directories, containing TEI XML files.
    TARGET must already contain catalog/catalog.xml (created by init_root_catalog or manually).
    """
    logging.basicConfig(
        level=log_level.upper(),
        format="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # --- Validate target catalog ---
    catalog_path = target / "catalog" / "catalog.xml"
    if not catalog_path.exists():
        logger.error(
            "Root catalog not found: %s — run init_root_catalog first or do it manually.", catalog_path
        )
        sys.exit(1)

    logger.info("Loading root catalog: %s", catalog_path)
    try:
        root_catalog = parse_xml(catalog_path)
    except Exception as exc:
        logger.error("Failed to parse root catalog %s: %s", catalog_path, exc)
        sys.exit(1)

    # --- Discover TEI folders ---
    tei_folders = sorted(p for p in source.iterdir() if p.is_dir())
    non_dirs = [p for p in source.iterdir() if not p.is_dir()]
    for p in non_dirs:
        logger.warning("Skipping non-directory entry in source: %s", p.name)

    if not tei_folders:
        logger.error("No sub-directories found in source: %s", source)
        sys.exit(1)

    logger.info("Found %d TEI folder(s) to process in %s", len(tei_folders), source)

    # --- Process each folder ---
    processed, failed = 0, 0
    for folder in tei_folders:
        logger.info("Processing folder: %s", folder.name)
        try:
            collection_elem = parse_teifolder_to_dapytains(
                source=folder,
                target=target,
                catalog=root_catalog,
            )
            root_catalog = append_collection_as_member_to_catalog(
                catalog_tree=root_catalog,
                collection=collection_elem,
            )
            logger.info("  ✓ %s", folder.name)
            processed += 1
        except Exception as exc:
            logger.error("  ✗ %s — %s", folder.name, exc, exc_info=True)
            failed += 1

    # --- Write updated root catalog ---
    logger.info("Writing updated root catalog → %s", catalog_path)
    try:
        write_xml_with_schema(root_catalog, catalog_path)
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
