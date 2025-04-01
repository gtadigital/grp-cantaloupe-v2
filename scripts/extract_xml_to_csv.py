import os
import csv
import xml.etree.ElementTree as ET
import argparse

# Define the namespace
# namespace is not defined consistently in all the files
NAMESPACE = {'ns': '*'}

def extract_data_from_xml(xml_file):
    """Extract _system_object_id and download_url from an XML file."""
    try:
        tree = ET.parse(xml_file)
        root = tree.getroot()

        # Extract _system_object_id
        _id = root.find(".//ns:do_grpm_06/ns:_id", NAMESPACE)
        if _id is None:
            print("!! There's no _id", xml_file)
            return None

        _id = _id.text.strip()

        # Find all <version> elements inside <versions> with @name='original'
        versions = root.findall(".//ns:do_grpm_06/ns:do_digitalobject/ns:files/ns:file/ns:versions/ns:version[@name='original']", NAMESPACE)
        
        download_url = None
        for version in versions:
            class_elem = version.find("ns:class", NAMESPACE)
            if class_elem is not None and class_elem.text.strip() == "image":
                download_url_elem = version.find("ns:download_url", NAMESPACE)
                if download_url_elem is not None and download_url_elem.text.strip():
                    download_url = download_url_elem.text.strip()
                    break  # Stop at the first valid image version

        if not download_url:
            print(f"!! No valid image download_url found in {xml_file}")
            return None
        
        filename = xml_file.rsplit('/', 1)[-1]  # Takes the element after the last slash

        return _id, download_url, filename

    except Exception as e:
        print(f"Error processing {xml_file}: {e}")
        return None


def process_directory(input_dir, output_dir):
    """Process all XML files in the directory and save results to CSV."""
    filename = "id_url_table.csv"
    output_csv = os.path.join(output_dir, filename)
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["_id", "image_url", "filename"])  # Write header

        for filename in os.listdir(input_dir):
            if filename.endswith(".xml"):
                file_path = os.path.join(input_dir, filename)
                extracted_data = extract_data_from_xml(file_path)

                if extracted_data:
                    writer.writerow(extracted_data)

    print(f"CSV file saved: {output_csv}")


if __name__ == "__main__":
    # Argument parser
    parser = argparse.ArgumentParser(description="Extract system_object_id and download_url from XML files and save to CSV.")
    parser.add_argument("input_dir", help="Directory containing XML files")
    parser.add_argument("output_dir", help="Output CSV directory")

    args = parser.parse_args()

    process_directory(args.input_dir, args.output_dir)
