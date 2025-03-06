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
        system_object_id = root.find(".//ns:do_grpm_06/ns:_system_object_id", NAMESPACE)
        if system_object_id is None:
            print("!! There's no system_object_id", xml_file)
            return None

        system_object_id = system_object_id.text.strip()

        # Extract download_url
        download_url_elem = root.find(".//ns:do_grpm_06/ns:do_digitalobject/ns:files/ns:file/ns:versions/ns:version[@name='original']/ns:download_url", NAMESPACE)
        if download_url_elem is None:
            print("!! There's no download_url")
            return None

        download_url = download_url_elem.text.strip()

        # Construct final ID
        final_id = f"https://resource.gta.arch.ethz.ch/digitalobject/cms-{system_object_id}"

        return final_id, download_url

    except Exception as e:
        print(f"Error processing {xml_file}: {e}")
        return None


def process_directory(input_dir, output_dir):
    """Process all XML files in the directory and save results to CSV."""
    filename = "id_url_table.csv"
    output_csv = os.path.join(output_dir, filename)
    with open(output_csv, mode="w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(["id", "image"])  # Write header

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
