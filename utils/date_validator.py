import re
# from utils.constants import replacements
def extract_date_tokens(date_format_str, replacements):
    # Create a regex pattern to match all known replacement keys
    keys = sorted(replacements.keys(), key=len, reverse=True)
    escaped_keys = map(re.escape, keys)
    pattern = '|'.join(escaped_keys)
    combined_pattern = re.compile(f'({pattern})')

    # Split the input string into potential tokens by non-alphanumeric separators
    parts = re.split(r'[^a-zA-Z0-9]+', date_format_str)

    # Validate each part
    result = []
    invalid_tokens = []

    for part in parts:
        if not part:
            continue
        match = combined_pattern.fullmatch(part)
        if match:
            result.append(match.group(1))
        else:
            invalid_tokens.append(part)

    if invalid_tokens:
        raise ValueError(f"Invalid token(s): {invalid_tokens}. Allowed tokens: {list(replacements.keys())}")

    # return result