"""
Script to extract identifiers and image download URLs from XML files and save the results to a CSV file.

This script parses XML files in a specified input directory, looks for the `_id` and associated 
download URL of image files marked as "original", and writes the extracted data into a CSV file 
named `id_url_table.csv` in the specified output directory.

Note:
    - The XML files may or may not declare consistent namespaces. This script uses a wildcard namespace selector.
    - Only the first valid image version per file is extracted (i.e., version with name='original' and class='image').

Usage:
    python extract_ids_and_urls.py /path/to/input_dir /path/to/output_dir

Arguments:
    - input_dir: Directory containing input XML files.
    - output_dir: Directory where the resulting CSV file will be saved.

CSV Output:
The script generates a CSV file with the following columns:
    - _id: Unique identifier extracted from each XML file
    - image_url: URL of the downloadable image
    - filename: The original XML filename (useful for traceability)
"""


import os
import csv
import xml.etree.ElementTree as ET
import argparse
from tqdm import tqdm
from utils.logger_helper import setup_logger

# Define the namespace
# namespace is not defined consistently in all the files
NAMESPACE = {'ns': '*'}

logger = setup_logger()

def get_download_url_from_versions(versions, file_class):
    for version in versions:
        class_elem = version.find("ns:class", NAMESPACE)
        if class_elem is not None and class_elem.text.strip() == file_class:
            download_url_elem = version.find("ns:download_url", NAMESPACE)
            if download_url_elem is not None and download_url_elem.text.strip():
                return download_url_elem.text.strip()
    return None

def extract_data_from_xml(xml_file):
    """Extract _id and download_url from an XML file."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Extract _id
        _id = root.find(".//ns:do_grpm_06/ns:_id", NAMESPACE)
        if _id is None:
            logger.warning("!! There's no _id", xml_file)
            return None

        _id = _id.text.strip()

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
            logger.info(f"No valid image download_url found in {xml_file}")
            return None
        
        # Takes the element after the last slash
        filename = xml_file.rsplit('/', 1)[-1]

        return _id, filename, download_url, download_office_url

    except Exception as e:
        logger.error(f"Error processing {xml_file}: {e}")
        return None


def process_directory(input_dir, output_dir):
    """Process all XML files in the directory and save results to CSV."""
    filename = "id_url_table.csv"
    output_csv = os.path.join(output_dir, filename)
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["_id", "filename", "image_url", "pdf_url"])  # Write header
        
        xml_files = [f for f in os.listdir(input_dir) if f.endswith(".xml")]

        for filename in tqdm(xml_files, desc="Processing XML files", unit="file"):
            file_path = os.path.join(input_dir, filename)
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
