import xml.etree.ElementTree as ET
import pyarrow as pa
from datetime import datetime
from utils import get

def process_catalogue():
    """Fetch and return ALL Eurostat catalogue metadata from XML"""
    # Fetch the XML table of contents
    url = "https://ec.europa.eu/eurostat/api/dissemination/catalogue/toc/xml"
    response = get(url, timeout=120.0)
    response.raise_for_status()
    
    # Parse XML
    root = ET.fromstring(response.content)
    
    # Define namespace
    ns = {'nt': 'urn:eu.europa.ec.eurostat.navtree'}
    
    # Extract all datasets (leaf nodes)
    all_datasets = []
    
    def extract_datasets(element, path=""):
        """Recursively extract all dataset information"""
        # Get current branch/leaf info
        code = element.find('nt:code', ns)
        code_text = code.text if code is not None else ""
        
        # Get title in English only
        title_en = ""
        for title in element.findall('nt:title', ns):
            if title.get('language', 'en') == 'en':
                title_en = title.text
                break
        
        # Build path for hierarchy
        current_path = f"{path}/{code_text}" if path else code_text
        
        # Check if this is a leaf (dataset)
        if element.tag == f"{{{ns['nt']}}}leaf":
            dataset = {
                'code': code_text,
                'path': current_path,
                'type': element.get('type', ''),
                'title': title_en,
            }
            
            # Add temporal metadata (convert DD.MM.YYYY to YYYY-MM-DD)
            dataset['last_update'] = datetime.strptime(element.find('nt:lastUpdate', ns).text, '%d.%m.%Y').strftime('%Y-%m-%d')
            dataset['last_modified'] = datetime.strptime(element.find('nt:lastModified', ns).text, '%d.%m.%Y').strftime('%Y-%m-%d')
                
            data_start = element.find('nt:dataStart', ns)
            if data_start is not None:
                dataset['data_start'] = data_start.text
                
            data_end = element.find('nt:dataEnd', ns)
            if data_end is not None:
                dataset['data_end'] = data_end.text
            
            # Add data volume
            values = element.find('nt:values', ns)
            if values is not None:
                dataset['values'] = int(values.text) if values.text else 0
            
            # Add source info (English only)
            source_en = ""
            for source in element.findall('nt:source', ns):
                if source.get('language', 'en') == 'en':
                    source_en = source.text
                    break
            if source_en:
                dataset['source'] = source_en
            
            # Add unit info (English only)
            unit_en = ""
            for unit in element.findall('nt:unit', ns):
                if unit.get('language', 'en') == 'en':
                    unit_en = unit.text if unit.text else ''
                    break
            if unit_en:
                dataset['unit'] = unit_en
            
            # Add description (English only)
            description_en = ""
            for desc in element.findall('nt:shortDescription', ns):
                if desc.get('language', 'en') == 'en':
                    description_en = desc.text if desc.text else ''
                    break
            if description_en:
                dataset['description'] = description_en
            
            # Add metadata links
            metadata_links = []
            for metadata in element.findall('nt:metadata', ns):
                format_type = metadata.get('format', '')
                link = metadata.text
                if link:
                    metadata_links.append(f"{format_type}:{link}")
            dataset['metadata_links'] = '|'.join(metadata_links) if metadata_links else ''
            
            # Add download link
            download_link = element.find('nt:downloadLink', ns)
            if download_link is not None:
                dataset['download_link'] = download_link.text
                dataset['download_format'] = download_link.get('format', '')
            
            all_datasets.append(dataset)
        
        # Process children recursively
        children = element.find('nt:children', ns)
        if children is not None:
            for child in children:
                extract_datasets(child, current_path)
    
    # Start extraction from root
    for branch in root:
        extract_datasets(branch)
    
    print(f"Loaded {len(all_datasets):,} datasets from Eurostat catalogue")
    
    # Return all data as PyArrow table
    return pa.Table.from_pylist(all_datasets)