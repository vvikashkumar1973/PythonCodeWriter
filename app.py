# app.py
import csv
import io
import os # Added for potential future use
from flask import Flask, request, jsonify, render_template, url_for # Added render_template

app = Flask(__name__)

# === Define Target Schema (same as before) ===
targetSchema = ['id', 'firstName', 'lastName', 'email', 'product', 'quantity', 'price']
# ===

# --- Route to Serve the Frontend UI ---
@app.route('/')
def index():
    # Renders the main HTML page from the templates folder
    return render_template('index.html')

# --- API Endpoint to Extract Headers ---
@app.route('/api/extract-headers', methods=['POST'])
def extract_headers():
    if 'csvfile' not in request.files:
        return jsonify({"error": "No file part"}), 400
    file = request.files['csvfile']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file and file.filename.lower().endswith('.csv'):
        try:
            # Ensure reading as text. App Engine environment might handle streams differently.
            # Reading directly into memory is fine for moderate file sizes to get headers.
            content = file.read().decode('utf-8-sig') # Use utf-8-sig to handle potential BOM
            stream = io.StringIO(content)
            csv_reader = csv.reader(stream)
            headers = next(csv_reader)
            # Basic sanitization/cleanup of headers
            headers = [h.strip() for h in headers]
            return jsonify({"headers": headers})
        except StopIteration:
             return jsonify({"error": "CSV file appears to be empty or has no headers"}), 400
        except UnicodeDecodeError:
            return jsonify({"error": "Could not decode file. Please ensure it's UTF-8 encoded."}), 400
        except Exception as e:
            app.logger.error(f"Error processing CSV headers: {e}") # Log error on GCP
            return jsonify({"error": f"Error processing CSV: {e}"}), 500
    else:
        return jsonify({"error": "Invalid file type, please upload a CSV"}), 400

# --- Code Generation Functions (generate_python_code, generate_javascript_code - same as before) ---
def generate_python_code(mapping, headers):
    # ... (Paste the Python code generation function from the previous answer here) ...
    # Ensure targetSchema is accessible here if needed
    code = f"""
import csv
import io # If reading from a stream/string later

def process_csv_data(csv_content_string): # Example: process from string
    \"\"\"Processes CSV data provided as a string.\"\"\"
    data = []
    try:
        stream = io.StringIO(csv_content_string)
        # Use DictReader for easier access by header name
        reader = csv.DictReader(stream)
        
        # Optional: Strict header check (adjust as needed)
        # expected_headers = set({list(headers)}) # Headers from the original upload
        # if set(reader.fieldnames) != expected_headers:
        #    print(f"Warning: CSV headers {{reader.fieldnames}} don't match expected {{expected_headers}}.")
            # Decide how to handle mismatch (error, proceed with caution, etc.)

        for row in reader:
            mapped_row = {{}}
"""
    # Add mapping logic
    for csv_header, target_field in mapping.items():
        code += f"            # Map '{csv_header}' to '{target_field}'\n"
        code += f"            if '{csv_header}' in row:\n"
        code += f"                mapped_row['{target_field}'] = row['{csv_header}']\n"
        code += f"            else:\n"
        code += f"                # Handle missing column - assign None, default, or skip field\n"
        code += f"                mapped_row['{target_field}'] = None \n"

    # Add logic for unmapped target fields (optional)
    all_target_fields = set(targetSchema) # Get all potential target fields
    mapped_target_fields = set(mapping.values())
    unmapped_targets = all_target_fields - mapped_target_fields
    if unmapped_targets:
         code += "\n            # Assign default values for unmapped target fields\n"
         for target_field in unmapped_targets:
             code += f"            if '{target_field}' not in mapped_row:\n" # Ensure it wasn't somehow mapped implicitly
             code += f"                 mapped_row['{target_field}'] = None # Or specific default\n"


    code += f"""
            data.append(mapped_row)

    except Exception as e:
        print(f"An error occurred during CSV processing: {{e}}")
        # Optionally re-raise the exception: raise
        return [] # Return empty list on error

    return data

# Example usage (assuming you have the CSV content in a variable 'csv_data'):
# processed_data = process_csv_data(csv_data)
# for item in processed_data:
#     print(item)

# --- To read from a file instead: ---
# def process_csv_file(filename='input.csv'):
#     data = []
#     try:
#         with open(filename, mode='r', encoding='utf-8-sig') as infile:
#              # Remainder of DictReader logic from above...
#     except FileNotFoundError:
#          print(f"Error: File '{{filename}}' not found.")
#          return []
#     except Exception as e:
#          print(f"An error occurred: {{e}}")
#          return []
#     return data
# processed_data_from_file = process_csv_file('your_uploaded_file.csv')
"""
    return code


def generate_javascript_code(mapping, headers):
    # ... (Paste the JavaScript code generation function from the previous answer here) ...
    # Make sure it generates runnable Node.js code assuming csv-parser might be used
    code = f"""
// Requires 'csv-parser' library: npm install csv-parser
const fs = require('fs');
const csv = require('csv-parser');
const stream = require('stream');
const { promisify } = require('util');
const pipeline = promisify(stream.pipeline); // For async stream handling

async function processCsvData(csvContentString) {{
    // Processes CSV data provided as a string
    const data = [];
    const readableStream = stream.Readable.from(csvContentString);
    const expectedHeaders = {headers}; // JS array from Python list

    const csvStream = csv()
        .on('headers', (headers) => {{
            // Optional: Validate headers
            const actualHeaders = new Set(headers);
            const expectedHeadersSet = new Set(expectedHeaders);
            if (expectedHeadersSet.size !== actualHeaders.size || ![...expectedHeadersSet].every(h => actualHeaders.has(h))) {{
                console.warn(`Warning: CSV headers ${{headers}} don't perfectly match expected headers ${{expectedHeaders}}.`);
                // Decide if this is a critical error or just a warning
            }}
            console.log('CSV Headers:', headers);
        }})
        .on('data', (row) => {{
            const mappedRow = {{}};
"""
    # Add mapping logic
    for csv_header, target_field in mapping.items():
        # Handle potential special characters in JS keys if necessary
        code += f"            // Map '{csv_header}' to '{target_field}'\n"
        code += f"            if (row.hasOwnProperty('{csv_header}')) {{\n"
        code += f"                mappedRow['{target_field}'] = row['{csv_header}'];\n"
        code += f"            }} else {{\n"
        code += f"                // Handle missing column - assign null, default, or skip field\n"
        code += f"                mappedRow['{target_field}'] = null;\n"
        code += f"            }}\n"

    # Add logic for unmapped target fields (optional)
    # Convert Python's targetSchema to JS array representation
    target_schema_js = repr(targetSchema) # Gets a string like "['id', 'firstName', ...]"
    code += f"""
            // Assign default values for unmapped target fields
            const allTargetFields = new Set({target_schema_js});
            const mappedTargetFields = new Set({list(mapping.values())});
            allTargetFields.forEach(field => {{
                if (!mappedTargetFields.has(field) && !mappedRow.hasOwnProperty(field)) {{
                     mappedRow[field] = null; // Or specific default
                }}
            }});

            data.push(mappedRow);
        }});

    try {{
        await pipeline(readableStream, csvStream);
        console.log('CSV data successfully processed.');
        return data;
    }} catch (error) {{
        console.error('Error processing CSV stream:', error);
        throw error; // Re-throw the error to be caught by the caller
    }}
}}

// Example usage (assuming you have the CSV content in a variable 'csvData'):
// async function run() {{
//   try {{
//     const processedData = await processCsvData(csvData);
//     processedData.forEach(item => console.log(item));
//   }} catch (err) {{
//     console.error('Failed:', err);
//   }}
// }}
// run();

// --- To read from a file instead: ---
// async function processCsvFile(filename = 'input.csv') {{
//    const data = [];
//    const readableStream = fs.createReadStream(filename);
//    // ... rest of the pipeline logic from processCsvData ...
//    // Add error handling for fs.createReadStream if needed
// }}
// processCsvFile('your_uploaded_file.csv').then(...).catch(...);
"""
    return code


# --- API Endpoint for Code Generation ---
@app.route('/api/generate-code', methods=['POST'])
def generate_code_endpoint():
    data = request.get_json()
    if not data or 'mapping' not in data or 'headers' not in data or 'language' not in data:
        return jsonify({"error": "Missing mapping, headers, or language in request"}), 400

    mapping = data['mapping']
    headers = data['headers'] # Original headers from upload
    language = data['language']

    # Basic validation of mapping keys against headers (optional but recommended)
    for csv_header in mapping.keys():
        if csv_header not in headers:
             app.logger.warning(f"Mapped header '{csv_header}' not found in original headers {headers}")
             # Decide how to handle: ignore, error, etc. Here we'll proceed.

    try:
        if language == 'python':
            generated_code = generate_python_code(mapping, headers)
        elif language == 'javascript':
            generated_code = generate_javascript_code(mapping, headers)
        else:
            return jsonify({"error": "Unsupported language requested"}), 400

        return jsonify({"code": generated_code})
    except Exception as e:
        app.logger.error(f"Error during code generation: {e}")
        return jsonify({"error": "Failed to generate code"}), 500

# Needed for App Engine deployment (or running locally with gunicorn)
# if __name__ == '__main__':
#     app.run(host='127.0.0.1', port=8080, debug=True)
# For App Engine, we typically don't include the __main__ block
# as the entrypoint is defined in app.yaml (often using gunicorn)
