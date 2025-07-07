import re

# Patterns for common titles followed by personal names (e.g. "mr. Jansen")
_TITLE_NAME_PATTERN = re.compile(
    r"(?i)\b(mr\.?|prof\.?|dr\.?|ir\.?)\s+((?:[A-Z]\.)+\s*)?"
    r"[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+(?:\s+[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+){0,2}"
)

# "klager" and "verweerder" parties followed by a name
_PARTY_PATTERN = re.compile(
    r"(?i)\b(klager|verweerder)\s+((?:[A-Z]\.)?\s*[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+)"
)

# Courtesy titles such as "de heer" or "mevrouw" followed by a name
_COURTESY_PATTERN = re.compile(
    r"(?i)\b(de\s+heer|mevrouw|mevr\.?)\s+((?:[A-Z]\.)+\s*)?"
    r"[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+(?:\s+[A-ZÀ-ÖØ-öø-ÿ][A-Za-zÀ-ÖØ-öø-ÿ.'`-]+){0,2}"
)

# Simple gemachtigde pattern: match a few tokens following the keyword
_GEMACHTIGDE_PATTERN = re.compile(
    r"(?i)(gemachtigde[^\n]{0,10}?(?:mr\.?\s*)?)((?:[A-Za-zÀ-ÖØ-öø-ÿ.'`-]+\s*){1,5})"
)

def scrub_title_names(text: str) -> str:
    """Replace titles followed by names with a placeholder."""
    if not text:
        return text
    return _TITLE_NAME_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_party_names(text: str) -> str:
    """Replace 'klager' or 'verweerder' names with a placeholder."""
    if not text:
        return text
    return _PARTY_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_courtesy_names(text: str) -> str:
    """Replace courtesy titles followed by names with a placeholder."""
    if not text:
        return text
    return _COURTESY_PATTERN.sub(lambda m: f"{m.group(1)} NAAM", text)

def scrub_gemachtigde_names(text: str) -> str:
    """Replace names following 'gemachtigde' with 'NAAM'."""
    if not text:
        return text
    return _GEMACHTIGDE_PATTERN.sub(lambda m: f"{m.group(1)}NAAM", text)

def scrub_text(text: str) -> str:
    """Apply all available name scrubbing rules."""
    if not text:
        return text
    text = scrub_title_names(text)
    text = scrub_party_names(text)
    text = scrub_courtesy_names(text)
    text = scrub_gemachtigde_names(text)
    return text
