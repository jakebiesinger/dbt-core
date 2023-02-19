import re
import dbt.exceptions
from typing import Any, Dict, Literal, Optional, Tuple
import yaml

# the C version is faster, but it doesn't always exist
try:
    from yaml import CLoader as Loader, CSafeLoader as SafeLoader, CDumper as Dumper
except ImportError:
    from yaml import Loader, SafeLoader, Dumper  # type: ignore  # noqa: F401

FRONTMATTER_DELIMITER = re.compile(r"^---\s*$", re.MULTILINE)
NON_WHITESPACE = re.compile(r"\S")

YAML_ERROR_MESSAGE = """
Syntax error near line {line_number}
------------------------------
{nice_error}

Raw Error:
------------------------------
{raw_error}
""".strip()


def line_no(i, line, width=3):
    line_number = str(i).ljust(width)
    return "{}| {}".format(line_number, line)


def prefix_with_line_numbers(string, no_start, no_end):
    line_list = string.split("\n")

    numbers = range(no_start, no_end)
    relevant_lines = line_list[no_start:no_end]

    return "\n".join([line_no(i + 1, line) for (i, line) in zip(numbers, relevant_lines)])


def contextualized_yaml_error(raw_contents, error):
    mark = error.problem_mark

    min_line = max(mark.line - 3, 0)
    max_line = mark.line + 4

    nice_error = prefix_with_line_numbers(raw_contents, min_line, max_line)

    return YAML_ERROR_MESSAGE.format(
        line_number=mark.line + 1, nice_error=nice_error, raw_error=error
    )


def safe_load(contents) -> Optional[Dict[str, Any]]:
    return yaml.load(contents, Loader=SafeLoader)


def load_yaml_text(contents, path=None):
    try:
        return safe_load(contents)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if hasattr(e, "problem_mark"):
            error = contextualized_yaml_error(contents, e)
        else:
            error = str(e)

        raise dbt.exceptions.DbtValidationError(error)    

def parse_yaml_frontmatter(content: str, on_error: Literal['warn_or_error', 'ignore']) -> Tuple[Optional[dict[str, Any]], str]:
    """Attempts to parse the YAML Frontmatter from `content`, returning a tuple of the parsed content and the remainder of the string.

    Frontmatter is defined as a block of YAML between two `---` tokens in an otherwise non-YAML document.
    
    The frontmatter must be placed at the beginning of the file: if anything but whitespace is present before the `---`, no attempt to
    parse will be made. If matching `---` blocks are found, we attempt to parse the string slice between them. If this is not valid YAML,
    the behavior indicated in `on_error` will be followed, with `ignore` and `warn` returning the original `content` string.
    """
    parts = FRONTMATTER_DELIMITER.split(content, 2)
    if len(parts != 3) or NON_WHITESPACE.search(parts[0]) is not None:
        # No frontmatter section or non-whitespace preceding the first `---`, so skip frontmatter
        return None, content
    
    yaml_content, after_footer = parts[1:]
    
    try:
        parsed_yaml = safe_load(yaml_content)
    except (yaml.scanner.ScannerError, yaml.YAMLError) as e:
        if on_error == 'warn_or_error':
            if hasattr(e, "problem_mark"):
                error = contextualized_yaml_error(content, e)
            else:
                error = str(e)
            error = f'Error parsing YAML frontmatter!{error}'
            dbt.events.functions.warn_or_error(dbt.exceptions.DbtValidationError(error))
        return None, content
    
    return parsed_yaml, after_footer

def maybe_has_yaml_frontmatter(content: str) -> bool:
    """Return if `content` *might* have YAML frontmatter

    This weak filter allows us to skip the more-expensive YAML parsing (which has to take place even if we're not using the frontmatter).
    """
    return FRONTMATTER_DELIMITER.search(content)
    




