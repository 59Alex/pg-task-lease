#!/usr/bin/env python3
"""Validate local Markdown/image links, GitHub heading anchors, and no guide website."""
from pathlib import Path
import re
import unicodedata

root = Path(__file__).resolve().parent.parent

def anchors(text):
    result = set(re.findall(r'<a\s+id=["\']([^"\']+)', text))
    counts = {}
    for heading in re.findall(r'^#{1,6}\s+(.+?)\s*#*$', text, re.M):
        heading = re.sub(r'<[^>]+>', '', heading).strip().lower()
        # GitHub removes punctuation/symbols while retaining Unicode letters, digits, '_' and '-'.
        slug = ''.join(c for c in heading if c in '_- ' or unicodedata.category(c)[0] in 'LN').replace(' ', '-')
        count = counts.get(slug, 0)
        result.add(slug + (f'-{count}' if count else ''))
        counts[slug] = count + 1
    return result

errors = []
files = [root / 'README.md', *sorted((root / 'docs').rglob('*.md'))]
for file in files:
    text = file.read_text()
    links = re.findall(r'\]\(([^)\s]+)(?:\s+"[^"]*")?\)', text)
    links += re.findall(r'(?:src|srcset)="([^"]+)"', text)
    for target in links:
        if re.match(r'^[a-zA-Z][a-zA-Z0-9+.-]*:', target):
            continue
        path, _, anchor = target.partition('#')
        destination = (file.parent / path).resolve() if path else file
        if not destination.exists():
            errors.append(f'{file.relative_to(root)}: missing {target}')
        elif anchor and destination.suffix == '.md' and anchor not in anchors(destination.read_text()):
            errors.append(f'{file.relative_to(root)}: missing anchor {target}')
for file in (root / 'docs').rglob('*'):
    if file.suffix.lower() in {'.html', '.js', '.tsx'}:
        errors.append(f'Unexpected guide website artifact: {file.relative_to(root)}')
assert not errors, '\n'.join(errors)
print(f'Markdown navigation, image paths and anchors: PASS ({len(files)} files)')
