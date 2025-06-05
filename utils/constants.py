# Datetime valid tokens
replacements = {
    'yyyy': '%Y',
    'yy': '%y',
    'mm': '%m',
    'dd': '%d',
    'hh': '%H',
    'mi': '%M',
    'ss': '%S',
    'mon': '%b',
    'month': '%B',
    'am/pm': '%p',
}

# Datetime known formats
known_formats = [
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
    "%Y/%m/%d",
    "%d-%m-%Y",
    "%y/%m/%d",
    "%d-%b-%Y",
    "%Y.%m.%d"
]