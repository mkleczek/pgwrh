#!/usr/bin/env python3
"""Run pinned Quint checks and record source hashes, commands, and raw output."""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--verify', action='store_true', help='also exhaust focused finite graphs with TLC')
    parser.add_argument('--output', type=Path, help='log/manifest directory (default .build/spec)')
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    output = (args.output or root / '.build/spec').resolve()
    output.mkdir(parents=True, exist_ok=True)
    quint = os.environ.get('QUINT', 'quint')
    backend = os.environ.get('QUINT_BACKEND', 'rust')
    if backend not in ('rust', 'typescript'):
        parser.error('QUINT_BACKEND must be rust or typescript')
    version = subprocess.check_output([quint, '--version'], text=True).strip()
    if version != '0.32.0':
        parser.error(f'expected Quint 0.32.0, found {version!r}')
    models = ['spec/writer_handoff.qnt', 'spec/obligation_frontiers.qnt']
    files = models + ['spec/check.py', 'spec/tlc.json']
    manifest = {
        'recorded_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
        'quint': version, 'backend': backend,
        'sources': {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files},
        'checks': [],
    }
    commands = []
    for model in models:
        name = Path(model).stem
        commands.extend([
            (f'{name}-types', ['typecheck', model]),
            (f'{name}-tests', ['test', model, f'--backend={backend}', '--max-samples=1']),
        ])
    campaigns = [
        ('wal-failures', models[0], 'init', 'step', 1000, 150, 20261102,
         ['reachedNewEpoch', 'completedTransfers', 'releasedHistory', 'reinitializedReplica']),
        ('wal-healthy', models[0], 'init', 'healthyStep', 1000, 200, 20261103,
         ['reachedNewEpoch', 'completedTransfers', 'releasedHistory']),
        ('frontiers-failures', models[1], 'init', 'step', 300, 120, 20261102,
         ['repeatedHandoff', 'offlineProgress', 'collectedHistory', 'rejoined']),
        ('frontiers-progress', models[1], 'offlineInit', 'progressStep', 300, 160, 20261103,
         ['repeatedHandoff', 'offlineProgress', 'collectedHistory']),
    ]
    for label, model, init, step, samples, steps, seed, witnesses in campaigns:
        commands.append((label, ['run', model, f'--backend={backend}', f'--init={init}',
            f'--step={step}', '--invariant=safety', f'--max-samples={samples}',
            f'--max-steps={steps}', f'--seed={seed}', '--witnesses', *witnesses, '--verbosity=1']))
    if args.verify:
        for step in ['boundedStep', 'boundedRepeatedStep']:
            commands.append((f'tlc-{step}', ['verify', models[1], '--backend=tlc',
                f'--step={step}', '--invariant=safety', '--tlc-config=spec/tlc.json',
                '--server-endpoint=localhost:18822', '--verbosity=3']))
    failed = False
    for label, command in commands:
        print(f'Running {label}', flush=True)
        started = datetime.datetime.now(datetime.timezone.utc).isoformat()
        # No shell interpolation; preserve exact CLI arguments and failed output.
        result = subprocess.run([quint, *command], cwd=root, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT, text=True)
        text = result.stdout
        (output / f'{label}.log').write_text(text)
        record = {'name': label, 'arguments': command, 'started_at': started,
                  'returncode': result.returncode,
                  'log_sha256': hashlib.sha256(text.encode()).hexdigest()}
        record['witnesses'] = {
            name: {'witnessed': int(count), 'traces': int(total)}
            for name, count, total in re.findall(
                r'(\w+) was witnessed in (\d+) trace\(s\) out of (\d+) explored', text)
        }
        counts = re.search(r'(\d+) states generated, (\d+) distinct states found, (\d+) states left', text)
        if counts:
            record['tlc_states'] = dict(zip(['generated', 'distinct', 'remaining'], map(int, counts.groups())))
        manifest['checks'].append(record)
        failed = failed or result.returncode != 0
        manifest['passed'] = not failed
        (output / 'summary.json').write_text(json.dumps(manifest, indent=2) + '\n')
        print(text, end='', flush=True)
        if failed:
            break
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
