"""
Script to extract identifiers and image download URLs from XML files and save the results to a CSV file.

This script parses XML files in a specified input directory, looks for the `_system_object_id` and associated 
download URL of image files marked as "original", and writes the extracted data into a CSV file 
named `id_url_table.csv` in the specified output directory.

Note:
    - The XML files may or may not declare consistent namespaces. This script uses a wildcard namespace selector.
    - Only the first valid image version per file is extracted (i.e., version with name='original' and class='image').

Usage:
    python extract_system_object_ids_and_urls.py /path/to/input_dir /path/to/output_dir

Arguments:
    - input_dir: Directory containing input XML files.
    - output_dir: Directory where the resulting CSV file will be saved.

CSV Output:
The script generates a CSV file with the following columns:
    - _system_object_id: Unique identifier extracted from each XML file
    - image_url: URL of the downloadable image
    - filename: The original XML filename (useful for traceability)
"""


import os
import csv
import xml.etree.ElementTree as ET
import argparse
from utils.logger_helper import setup_logger

logger = setup_logger()

# Define the namespace
# namespace is not defined consistently in all the files
NAMESPACE = {'ns': '*'}


def get_download_url_from_versions(versions, file_class):
    for version in versions:
        class_elem = version.find("ns:class", NAMESPACE)
        if class_elem is not None and class_elem.text.strip() == file_class:
            download_url_elem = version.find("ns:download_url", NAMESPACE)
            if download_url_elem is not None and download_url_elem.text.strip():
                return download_url_elem.text.strip()
    return None

def extract_data_from_xml(xml_file):
    """Extract _system_object_id and download_url from an XML file."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Extract _system_object_id
        _system_object_id = root.find(".//ns:do_grpm_06/ns:_system_object_id", NAMESPACE)
        if _system_object_id is None:
            logger.warning("!! There's no _system_object_id", xml_file)
            return None

        _system_object_id = _system_object_id.text.strip()

        # Find all <version> elements inside <versions> with @name='original'
        versions = root.findall(".//ns:do_grpm_06/ns:do_digitalobject/ns:files/ns:file/ns:versions/ns:version[@name='original']", NAMESPACE)
        
        # Find the first <version> element with <class> = "image" and extract the <download_url> element
        download_url = get_download_url_from_versions(versions, "image")
        download_office_url = get_download_url_from_versions(versions, "office")

        # Check for pdfs
        if download_office_url is not None and download_office_url.endswith(".pdf"):
            preview_versions = root.findall(".//ns:do_grpm_06/ns:do_digitalobject/ns:files/ns:file/ns:versions/ns:version[@name='preview']", NAMESPACE)
            download_url = get_download_url_from_versions(preview_versions, "image")

        if not download_url:
            for version in versions:
                class_elem = version.find("ns:class", NAMESPACE)
            logger.warning(f"!! No valid image download_url found in {xml_file}")
            logger.info(f"!! file type: {class_elem.text.strip()}")
            return None
        
        # Takes the element after the last slash
        filename = xml_file.rsplit('/', 1)[-1]

        return _system_object_id, filename, download_url, download_office_url

    except Exception as e:
        logger.error(f"Error processing {xml_file}: {e}")
        return None


def process_directory(input_dir, output_dir):
    """Process all XML files in the directory and save results to CSV."""
    filename = "id_url_table.csv"
    output_csv = os.path.join(output_dir, filename)
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["_system_object_id", "filename", "image_url", "pdf_url"])  # Write header

        for filename in os.listdir(input_dir):
            if filename.endswith(".xml"):
                file_path = os.path.join(input_dir, filename)
                # Process XML file
                extracted_data = extract_data_from_xml(file_path)

                if extracted_data:
                    writer.writerow(extracted_data)

    logger.info(f"CSV file saved: {output_csv}")


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(description="Extract system_object_id and download_url from XML files and save to CSV.")
    parser.add_argument("input_dir", help="Directory containing XML files")
    parser.add_argument("output_dir", help="Output CSV directory")

    args = parser.parse_args()

    process_directory(args.input_dir, args.output_dir)
