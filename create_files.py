from pathlib import Path

from constants import pairs, intervals, headers


for pair in pairs:
    for interval in intervals:
        path = f'./data/{interval}/{pair}'
        Path(path).mkdir(parents=True, exist_ok=True)
        with open(path + '/1.csv', "w") as f:
            f.write(f'{",".join(headers)}\n')
