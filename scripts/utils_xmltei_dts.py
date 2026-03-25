import sys
import click
from copy import deepcopy
from pathlib import Path

from lxml import etree
from lxml.etree import Element, ElementTree

# XML-TEI
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
    root = deepcopy(tree.getroot())
    pi = etree.ProcessingInstruction("oxygen", 'RNGSchema="schema.rng" type="xml"')
    root.addprevious(pi)
    output_path.parent.mkdir(exist_ok=True, parents=True)
    etree.ElementTree(root).write(
        output_path, pretty_print=True, xml_declaration=True, encoding="UTF-8"
    )

# DTS
# ---
def append_collection_as_member_to_catalog(
    catalog_tree: ElementTree, collection: Element
) -> ElementTree:
    root = catalog_tree.getroot()
    members_element = root.find("members")
    if members_element is None:
        members_element = etree.SubElement(root, "members")
    members_element.append(collection)
    return catalog_tree