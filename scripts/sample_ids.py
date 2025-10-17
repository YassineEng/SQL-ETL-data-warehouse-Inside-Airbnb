import gzip
import glob
import json
from collections import Counter

files = glob.glob('data/cleaned_data/*listings*.csv*')
print('Found files:', files)
ids = []
for f in files[:3]:
    openf = gzip.open if f.endswith('.gz') else open
    try:
        with openf(f, 'rt', encoding='utf-8') as fh:
            header = fh.readline()
            for i, line in enumerate(fh):
                if i >= 500:
                    break
                parts = line.split('|')
                if parts:
                    ids.append(parts[0].strip())
    except Exception as e:
        print('Error reading', f, e)

lens = Counter(len(s) for s in ids)
chars = Counter(''.join(ids))

out = {
    'sample_files': files[:3],
    'sample_size': len(ids),
    'length_distribution_top10': lens.most_common(10),
    'chars_top20': chars.most_common(20),
    'examples_first20': ids[:20]
}
print(json.dumps(out, indent=2))
