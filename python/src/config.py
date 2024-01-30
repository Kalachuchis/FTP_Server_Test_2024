INVALID_FOLDER = "Invailid Files"
FAILED_FOLDER = "Failed Files"

DIR_REGEX = r"^[\(a-zA-Z0-9-_\s)+\/]+$"
SKIP_REGEX = [
    r"Input",
    r"Catalogued",
    r"Invalid",
    r"Failed",
    "For Processing",
]
OUTPUT_REGEX = r"\/Output"

#
BATCH_SPREADSHEET_NAME = r".*Batch KVP Spreadsheet\.xlsx"
SUB_SEARCHABLE_REGEX = ".*Searchable PDF/"
SUFFIX_REMOVE_SEARCHABLE = " - Searchable PDF.pdf"
