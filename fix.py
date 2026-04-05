with open('app.py', 'r') as f:
    text = f.read()

import re
# Find all occurrences of the waste_log create block
block = r'''    conn\.execute\(\s*"""\s*CREATE TABLE IF NOT EXISTS waste_log \([\s\S]*?FOREIGN KEY\(item_id\) REFERENCES items\(id\)\s*\)\s*"""\s*\)'''
matches = list(re.finditer(block, text))

if len(matches) > 1:
    text = text[:matches[1].start()] + text[matches[1].end():]
    with open('app.py', 'w') as f:
        f.write(text)
    print("Fixed duplicates.")
else:
    print("No duplicates.")
