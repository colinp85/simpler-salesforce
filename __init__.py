from simple_salesforce import Salesforce
import os
import logging
import yaml
import requests
import glob

    
# Set SSL library log level to ERROR to reduce verbose output
logging.getLogger('urllib3').setLevel(logging.ERROR)

sf = None

object_definitions = {}


def get_client():
    """
    Returns a Salesforce client instance, creating one if it doesn't exist.
    
    Returns:
        Salesforce: Authenticated Salesforce client instance.
    """
    global sf

    url = os.environ.get('SALESFORCE_TOKEN_URL')

    auth_data = {
        'grant_type': "client_credentials",
        'client_id': os.environ.get('CONSUMER_KEY'),
        'client_secret': os.environ.get('CONSUMER_SECRET')
    }

    if sf is None:
        resp = requests.post(url, data=auth_data).json()

        # Connect to Salesforce
        try:
            sf = Salesforce(instance_url=resp['instance_url'], session_id=resp['access_token'])
            logging.info("Connected to Salesforce successfully!")
            return sf
        except Exception as e:
            logging.error(f"Error connecting to Salesforce: {e}")
            exit(1)

    return sf


def get_all_objects():
    """
    Retrieve a list of all Salesforce object API names using simple_salesforce.
    Returns:
        list: List of object API names.
    """
    try:
        svc = get_client()
        all_objects = svc.describe()['sobjects']
        return [obj['name'] for obj in all_objects]
    except Exception as e:
        logging.error(f"Error retrieving all Salesforce objects: {e}")
        return []


def run_soql_query(query):
    """
    Executes a SOQL query using query_all and returns results as an OrderedDict.

    Args:
        query (str): The SOQL query string.

    Returns:
        OrderedDict or None: Query results as an OrderedDict, or None if an error occurs.
    """
    try:
        svc = get_client()
        results = svc.query_all(query)
        return results.get('records', [])
    except Exception as e:
        logging.error(f"Error running SOQL query: {e}")
        return []


def describe_object(object_name):
    """
    Retrieves the metadata description for a given Salesforce object.

    Args:
        object_name (str): The API name of the Salesforce object (e.g., 'Account', 'Contact').

    Returns:
        dict: The metadata description of the object, or None if not found.
    """
    try:
        sf = get_client()
        return sf.__getattr__(object_name).describe()
    except Exception as e:
        logging.error(f"Error describing Salesforce object '{object_name}': {e}")
        return None


def load_object_definitions(names=[], cache_folder=None, output=None):
    """
    Loads Salesforce object definitions either from Salesforce API or from cached YAML files.
    
    This function populates the global object_definitions dictionary with field metadata
    for specified Salesforce objects. It can either retrieve fresh data from Salesforce
    or load from previously cached YAML files for faster access.
    
    Args:
        names (list, optional): List of specific object API names to load. If empty or None,
            loads all available objects from Salesforce. Defaults to [].
        cache_folder (str, optional): Path to folder containing cached YAML files. If None,
            retrieves fresh data from Salesforce API. If provided, loads from cached files
            instead. Defaults to None.
        output (str, optional): Path to folder where object definitions should be saved as
            YAML files. If None, no files are written. Defaults to None.
    
    Returns:
        None: This function modifies the global object_definitions dictionary in-place.
    
    Examples:
        # Load all objects from Salesforce and cache to files
        load_object_definitions(output='./cache')
        
        # Load specific objects from Salesforce
        load_object_definitions(['Account', 'Contact'])
        
        # Load from cached files
        load_object_definitions(cache_folder='./cache')
        
        # Load specific objects from cache
        load_object_definitions(['Account'], cache_folder='./cache')
    """
    global object_definitions

    object_names = names if names else get_all_objects()

    if cache_folder is None:
        # retrieve from Salesforce
        for object_name in object_names:
            obj_desc = describe_object(object_name)
            if not obj_desc:
                logging.error(f"Object '{object_name}' not found or description failed.")
                continue

            fields = []
            for f in obj_desc.get('fields', []):
                field_info = {
                    'name': f.get('name'),
                    'label': f.get('label'),
                    'type': f.get('type'),
                    'reference': f.get('referenceTo', [])[0] if f.get('referenceTo') else None,
                    'length': f.get('length'),
                    'picklistValues': [pv['value'] for pv in f.get('picklistValues', [])]
                }
                fields.append(field_info)

            logging.debug(f"loaded object definition for {object_name} with {len(fields)} fields")
            object_definitions[object_name] = {field['name']: field for field in fields if 'name' in field}

            if output:
                os.makedirs(output, exist_ok=True)
                output_file = os.path.join(output, f"{object_name}.yaml")
                logging.debug("Writing object definition to YAML file: %s", output_file)
                with open(output_file, 'w') as f:
                    yaml.dump(fields, f, default_flow_style=False, sort_keys=False)

    else:
        # load from cache folder
        yaml_files = glob.glob(os.path.join(cache_folder, '*.yaml'))
        for yaml_file in yaml_files:
            object_name = os.path.splitext(os.path.basename(yaml_file))[0]
            if names is not None and object_name not in names:
                continue
            try:
                with open(yaml_file, 'r') as f:
                    fields = yaml.safe_load(f)
                    if isinstance(fields, list):
                        object_definitions[object_name] = {field['name']: field for field in fields if 'name' in field}
                        logging.debug(f"Loaded cached definition for {object_name} with {len(fields)} fields")
            except Exception as e:
                logging.error(f"Error loading cached YAML for {object_name}: {e}")


def get_object_fields(name):
    """
    Retrieves the fields for a given Salesforce object by its API name.
    If the object is not found in the loaded definitions, it queries Salesforce to get the fields.

    Args:
        name (str): The API name of the Salesforce object.

    Returns:
        dict: A dictionary of field names and their definitions.
    """
    if not object_definitions:
        logging.error("Object definitions not loaded. Call load_object_field_definitions first.")
        return None

    if name in object_definitions:
        return object_definitions[name]
    else:
        logging.error(f"Object '{name}' not found in loaded definitions.")
        return None


def create_object(object_name, data):
    """
    Creates a new Salesforce object record using the provided data.
    Args:
        object_name (str): The API name of the Salesforce object.
        data (dict): The data to create the object with.
    Returns:
        dict: The created object record.
    """
    try:
        svc = get_client()
        result = svc.__getattr__(object_name).create(data)
        logging.info(f"Created {object_name} with Id: {result['id']}")
        return result
    except Exception as e:
        logging.error(f"Error creating {object_name}: {e}")
        return None


def get_object(object_name, where=None):
    """
    Query Salesforce for objects by API name, using fields from the loaded YAML definitions.
    Args:
        object_name (str): The API name of the Salesforce object.
        where (str, optional): The WHERE clause (without 'WHERE').
    Returns:
        list: List of object records (dicts).
    """
    fields = get_object_fields(object_name)
    if not fields:
        logging.error(f"fields for object '{object_name}' not found.")
        return []
    field_list = ', '.join(fields.keys())
    query = f"SELECT {field_list} FROM {object_name}"
    if where:
        query += f" WHERE {where}"
    results = run_soql_query(query)
    return results if results else []


def get_object_by_id(object_name, id):
    """
    Query Salesforce for a single object by its API name and Id, using fields from the loaded YAML definitions.
    Args:
        object_name (str): The API name of the Salesforce object.
        id (str): The Salesforce Id to filter by.
    Returns:
        dict or None: The object record if found, else None.
    """
    results = get_object(object_name, where=f"Id = '{id}'")
    if results and len(results) > 0:
        return results[0]
    return None


def get_object_references(object_name):
    """
    Retrieves the reference fields for a given Salesforce object by its API name.

    Args:
        object_name (str): The API name of the Salesforce object.

    Returns:
        list: A list of reference field names.
    """
    fields = get_object_fields(object_name)
    if not fields:
        logging.error(f"Fields for object '{object_name}' not found.")
        return []

    references = {field['name']: field for field in fields.values() if field.get('reference')}
    return references


def resolve_references(obj, object_name, refs=None):
    """
    Resolves reference fields in a Salesforce object dict, replacing reference ids with the referenced object dicts.
    Args:
        obj (dict): The object record to resolve references for.
        object_name (str): The API name of the Salesforce object.
        refs (list, optional): List of reference field names to resolve. If None, resolve all references.
    Returns:
        dict: The object with references resolved (in-place).
    """
    reference_fields = get_object_references(object_name)
    if not reference_fields:
        logging.error(f"no reference fields found for object '{object_name}'.")
        return obj
    for field_name, field_info in reference_fields.items():
        if obj.get(field_name):
            if refs is not None and field_name not in refs:
                continue
            ref_obj_name = field_info['reference']
            ref_id = obj[field_name]
            ref_data = get_object_by_id(ref_obj_name, ref_id)
            if ref_data:
                resolved_key = field_name.replace('__c', '__r')
                obj[resolved_key] = ref_data
    return obj


def pretty_print_object(obj, object_name, indent=0):
    """
    Pretty prints a Salesforce object using the loaded YAML field definitions.
    Each field is printed as 'FieldLabel (FieldName): Value' on a new line.
    Falls back to the field name if label is not available.
    Args:
        obj (dict): The Salesforce object record to print.
        object_name (str): The API name of the Salesforce object.
        indent (int, optional): Number of spaces to indent each line. Defaults to 0.
    """
    fields = get_object_fields(object_name)
    if not fields:
        logging.error(f"fields for object '{object_name}' not found.")
        return
    prefix = ' ' * indent
    print(f"{prefix}---- Object: {object_name} ----")
    # Sort fields by label (fallback to field name if label missing)
    sorted_fields = sorted(fields.values(), key=lambda f: f.get('label', f['name']))
    for field_info in sorted_fields:
        field_name = field_info['name']
        label = field_info.get('label', field_name)
        value = obj.get(field_name, None)
        print(f"{prefix}{label} ({field_name}): {value}")


def upload_file(object_id, file_path):
    """
    Uploads a file to a Salesforce object.

    Args:
        object_id (str): The Salesforce object ID to which the file will be attached.
        file_path (str): The full path of the file to upload.

    Returns:
        dict: The response from Salesforce after the file upload.

    Example:
        response = upload_file('001XXXXXXXXXXXXXXX', '/path/to/file.pdf')
    """
    try:
        svc = get_client()
        file_name = os.path.basename(file_path)

        import base64
        with open(file_path, 'rb') as file_data:
            encoded_data = str(base64.b64encode(file_data.read()))
            response = svc.ContentVersion.create({
                'Title': os.path.splitext(file_name)[0],  # Exclude the file extension
                'PathOnClient': file_path,
                'VersionData': encoded_data[2:-1],
                'FirstPublishLocationId': object_id
            })

        logging.info(f"File '{file_name}' uploaded successfully to object ID '{object_id}'.")
        return response
    except Exception as e:
        logging.error(f"Failed to upload file '{file_path}' to object ID '{object_id}': {e}")
        raise
