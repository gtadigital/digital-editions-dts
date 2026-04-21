import os
import shutil
from copy import deepcopy
from pathlib import Path
from lxml import etree
from lxml.etree import ElementTree, Element

DIR_OUT = Path(Path.cwd().parent, "_dts_out")
NS = {"tei": "http://www.tei-c.org/ns/1.0"}


def parse_xml(path: Path | str) -> ElementTree:
    parser = etree.XMLParser(remove_blank_text=True, load_dtd=True)
    return etree.parse(str(path), parser)


def process_tei_folder_with_facsimile_leafs(
    tei_folder: Path,
    base_identifier: str,
    current_collection_identifier: str | None = None,
    current_catalog: Element | None = None,
) -> tuple[Element, int]:
    xml_files = sorted(tei_folder.glob("*.xml"))
    tei_subfolders = sorted(f for f in tei_folder.iterdir() if f.is_dir())

    manuscript_identifier = f"{base_identifier}/{tei_folder.name}"

    # -- Build the manuscript catalog at first recurrence --
    if current_catalog is None:
        print(f"Building Manuscript Catalog for {tei_folder.name}")
        manuscript_catalog = etree.Element(
            "collection",
            identifier=manuscript_identifier,
        )
        all_xml_files = sorted(tei_folder.glob("**/*.xml"))
        title_elem = etree.SubElement(manuscript_catalog, "title")
        first_tei_tree = parse_xml(all_xml_files[0])
        extracted_titles = first_tei_tree.xpath(
            "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
            namespaces=NS,
        )
        title_elem.text = (
            extracted_titles[0].text.strip() if extracted_titles else tei_folder.name
        )
        current_catalog = deepcopy(manuscript_catalog)
        current_collection_identifier = deepcopy(manuscript_identifier)

    # -- Find the current collection element in the catalog tree --
    # descendant-or-self handles both the root element and any nested collection
    matches = current_catalog.xpath(
        f"descendant-or-self::*[@identifier='{current_collection_identifier}']"
    )
    current_collection = matches[0]
    current_collection_members = etree.SubElement(current_collection, "members")

    # -- Add .xml files as resource elements --
    for xml_file in xml_files:
        xml_file_etree = parse_xml(xml_file)
        resource_identifier = f"{current_collection_identifier}/{xml_file.stem}"
        resource = etree.SubElement(
            current_collection_members,
            "resource",
            identifier=resource_identifier,
        )
        resource_title = etree.SubElement(resource, "title")
        tei_title_nodes = xml_file_etree.xpath(
            "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
            namespaces=NS,
        )
        resource_title.text = (
            "".join(tei_title_nodes[0].itertext())
            if tei_title_nodes
            else xml_file.stem
        )

    # -- Add subfolders as nested collection elements, then recurse --
    for subfolder in tei_subfolders:
        subfolder_identifier = f"{current_collection_identifier}/{subfolder.name}"
        subcollection_elem = etree.SubElement(
            current_collection_members,
            "collection",
            identifier=subfolder_identifier,
        )
        # Add title to the sub-collection from its first xml file
        sub_xml_files = sorted(subfolder.glob("**/*.xml"))
        if sub_xml_files:
            sub_title_elem = etree.SubElement(subcollection_elem, "title")
            sub_tei_tree = parse_xml(sub_xml_files[0])
            sub_titles = sub_tei_tree.xpath(
                "/tei:TEI/tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title",
                namespaces=NS,
            )
            sub_title_elem.text = (
                sub_titles[0].text.strip() if sub_titles else subfolder.name
            )
        # Recurse to populate this sub-collection's members
        process_tei_folder_with_facsimile_leafs(
            tei_folder=subfolder,
            base_identifier=base_identifier,
            current_collection_identifier=subfolder_identifier,
            current_catalog=current_catalog,
        )

    return current_catalog, 0


# --- Run ---
DIR_SAMPLE = Path(__file__).parent.parent / "_dev" / "test-data_dts"
base_folder = DIR_SAMPLE
base_identifier = "http://example/org/dts/semper/semper-edition"

base_tei_subfolders = sorted(p for p in base_folder.iterdir() if p.is_dir())
for p in base_folder.iterdir():
    if not p.is_dir():
        print(f"Skipping non-directory entry: {p.name}")

print(f"Found {len(base_tei_subfolders)} TEI folder(s) in '{base_folder}'\n")

for tei_folder in base_tei_subfolders:
    print(f"Processing folder: {tei_folder.stem}")
    ms_catalog, _ = process_tei_folder_with_facsimile_leafs(
        tei_folder=tei_folder,
        base_identifier=base_identifier,
    )
    print(etree.tostring(ms_catalog, pretty_print=True, encoding="unicode"))
