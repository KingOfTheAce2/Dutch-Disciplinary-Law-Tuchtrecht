# crawler/scrubber.py
# This module anonymizes names found in the case text.

import re

def scrub_text(text: str) -> str:
    """
    Applies light anonymization to the text.

    Args:
        text: The original case text.

    Returns:
        The anonymized text.
    """
    # Redact patterns like "mr. Jansen", "klager X", "verweerder Y"
    # Using function to replace to avoid re-compiling regex every time.
    # This is a simple implementation and might need refinement for edge cases.
    
    # "mr. [Titlecase Name]"
    text = re.sub(r'mr\.\s+([A-Z][a-z]+)', 'mr. [NAAM]', text)
    # "klager/verweerder [Single Letter or Name]"
    text = re.sub(r'(klager|verweerder)\s+([A-Z][a-zA-Z]*)', r'\1 [NAAM]', text)
    
    # A more general approach for names (can have false positives)
    # Looks for a capital letter word not at the start of a sentence.
    # This is tricky and can redact legitimate legal terms.
    # A more robust solution would involve a dictionary of legal terms to exclude.
    # For now, this is a placeholder for a more advanced implementation.
    
    return text
