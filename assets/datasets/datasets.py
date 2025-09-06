import pyarrow as pa
from utils import get

def product_of_sizes(sizes):
    """Calculate the product of all sizes"""
    product = 1
    for size in sizes:
        product *= size
    return product

def json_stat_to_pyarrow_table(json_stat):
    """
    Convert JSON-stat format to PyArrow table.
    
    Args:
        json_stat: JSON-stat formatted data
    
    Returns:
        PyArrow table with properly structured data
    """
    if "dimension" not in json_stat or "value" not in json_stat:
        raise ValueError("Invalid JSON-stat format")

    dimensions = json_stat["dimension"]
    dim_labels = json_stat["id"]
    
    values = json_stat["value"]
    is_value_array = isinstance(values, list)
    
    # Initialize lists for each column
    column_data = {label: [] for label in dim_labels}
    column_data["value"] = []
    
    total_data_points = len(values) if is_value_array else product_of_sizes(json_stat["size"])
    
    for i in range(total_data_points):
        # Calculate indices for each dimension
        indices = []
        temp_i = i
        for size in reversed(json_stat["size"]):
            indices.append(temp_i % size)
            temp_i //= size
        indices.reverse()
        
        # Get the dimension values for this data point
        for idx, label in enumerate(dim_labels):
            dim_key = dimensions[label]
            category_index = dim_key["category"]["index"]
            category_label = dim_key["category"]["label"]
            
            # Get the category key at this index position
            cat_keys = list(category_index.keys())
            cat_position = indices[idx]
            
            if cat_position < len(cat_keys):
                cat_key = cat_keys[cat_position]
                column_data[label].append(category_label.get(cat_key, cat_key))
            else:
                column_data[label].append(None)
        
        # Get the data value
        if is_value_array:
            data_value = values[i] if i < len(values) else None
        else:
            data_value = values.get(str(i), None)
        column_data["value"].append(data_value)
    
    # Create PyArrow table
    fields = [pa.field(label, pa.string()) for label in dim_labels] + [pa.field("value", pa.float64())]
    schema = pa.schema(fields)
    arrays = [pa.array(column_data[label]) for label in dim_labels] + [pa.array(column_data["value"])]
    table = pa.Table.from_arrays(arrays, schema=schema)
    
    return table

def process_dataset(dataset_code: str) -> pa.Table:
    """
    Process a single Eurostat dataset using JSON-stat API.
    
    Args:
        dataset_code: The code of the dataset to fetch (e.g., 'ei_bpm6ca_m')
    
    Returns:
        PyArrow table containing the dataset
    """
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}"
    
    # Request JSON format with English language
    params = {
        'format': 'JSON',
        'lang': 'EN'
    }
    
    response = get(url, params=params, timeout=300.0)
    response.raise_for_status()
    
    json_data = response.json()
    table = json_stat_to_pyarrow_table(json_data)
    
    print(f"Processed dataset {dataset_code}: {table.num_rows:,} rows")
    return table