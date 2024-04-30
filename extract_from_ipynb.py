''
# Attempting a different method to extract the Python code from the Jupyter notebook
import nbformat

# Define the path to the notebook file
notebook_file_path = r'C:\Users\lizal\Documents\LAB Tech\LT Consult\NewAtlantis Labs\Neo4J\src_semi\KG.ipynb'

# Function to extract Python code from a Jupyter notebook
def extract_python_code_from_notebook(notebook_path):
    with open(notebook_path, 'r', encoding='utf-8') as notebook_file:
        notebook = nbformat.read(notebook_file, as_version=4)
        # Filter out cells that are code cells and concatenate their content
        code_cells_content = [
            cell['source'] for cell in notebook.cells if cell.cell_type == 'code'
        ]
        return '\n\n'.join(code_cells_content)

# Extract the Python code
python_code = extract_python_code_from_notebook(notebook_file_path)

# Path for the output Python file
python_file_path = r'C:\Users\lizal\Documents\LAB Tech\LT Consult\NewAtlantis Labs\Neo4J\KG_extracted.py'

# Write the extracted Python code to the file
with open(python_file_path, 'w', encoding='utf-8') as python_file:
    python_file.write(python_code)


