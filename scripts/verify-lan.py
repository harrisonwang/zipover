#!/usr/bin/env python3
"""Build and exercise production pairing/discovery/transport on this Mac and an SSH host.

Uses isolated configuration on both hosts and synthetic clipboard payloads. No
user clipboard contents or pairing secrets are printed or uploaded to reports.
"""
import argparse
import json
import os
from pathlib import Path
import queue
import shlex
import subprocess
import threading
import time

ROOT = Path(__file__).resolve().parents[1]


def run(args, **kwargs):
    return subprocess.run(args, check=True, text=True, **kwargs)


def test_executable(output):
    for line in output.splitlines():
        item = json.loads(line)
        if item.get('reason') == 'compiler-artifact' and item.get('profile', {}).get('test') and item.get('executable') and item.get('target', {}).get('kind') == ['lib']:
            return item['executable']
    raise RuntimeError('cargo did not report the library test executable')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--host', default='hp')
    parser.add_argument('--remote-dir', default='/tmp/zipover-acceptance')
    args = parser.parse_args()
    stamp = str(int(time.time()))
    artifacts = ROOT / 'target' / 'lan-acceptance' / stamp
    artifacts.mkdir(parents=True)
    remote_config = '/tmp/zipover-lan-' + stamp
    local_env = dict(os.environ, ZIPOVER_CONFIG_DIR=str(artifacts / 'config'), ZIPOVER_TEST_ROLE='mac')
    remote_dir = shlex.quote(args.remote_dir)

    def ssh(command, **kwargs):
        return run(['ssh', '-o', 'BatchMode=yes', args.host, command], **kwargs)

    local_test = test_executable(run(['cargo', 'test', '--locked', '-p', 'zipover-core', '--lib', '--no-run', '--message-format=json'], cwd=ROOT, capture_output=True).stdout)
    ssh('mkdir -p ' + remote_dir)
    run(['rsync', '-azR', 'Cargo.toml', 'Cargo.lock', '.cargo', 'crates', 'assets', args.host + ':' + args.remote_dir + '/'], cwd=ROOT)
    prefix = 'cd ' + remote_dir + ' && PATH="$HOME/.cargo/bin:$PATH" '
    remote_test = test_executable(ssh(prefix + 'cargo test --locked -p zipover-core --lib --no-run --message-format=json', capture_output=True).stdout)
    remote_env = 'ZIPOVER_CONFIG_DIR=' + shlex.quote(remote_config) + ' ZIPOVER_TEST_ROLE=hp '
    processes = []
    logs = []
    reader = None
    try:
        test_args = 'sync::acceptance::lan_round_trip --ignored --exact --nocapture'
        mac_log = (artifacts / 'transfer-mac.log').open('w'); logs.append(mac_log)
        local = subprocess.Popen([local_test, *test_args.split()], env=local_env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True); processes.append(local)
        codes = queue.Queue()

        def capture_local():
            for line in local.stdout:
                if line.startswith('ZIPOVER_TEST_CODE='):
                    codes.put(line.removeprefix('ZIPOVER_TEST_CODE=').strip())
                else:
                    mac_log.write(line)
                    mac_log.flush()

        reader = threading.Thread(target=capture_local)
        reader.start()
        code = codes.get(timeout=10)
        assert len(code) == 6 and code.isascii() and code.isdigit(), 'expected six-digit code'
        hp_log = (artifacts / 'transfer-hp.log').open('w'); logs.append(hp_log)
        remote = subprocess.Popen(['ssh', '-o', 'BatchMode=yes', args.host, 'cd ' + remote_dir + ' && timeout 75s env ' + remote_env + shlex.quote(remote_test) + ' ' + test_args], stdin=subprocess.PIPE, stdout=hp_log, stderr=subprocess.STDOUT, text=True); processes.append(remote)
        remote.stdin.write(code + '\n')
        remote.stdin.close()
        del code
        assert local.wait(timeout=70) == 0, 'Mac transfer test failed (see log)'
        assert remote.wait(timeout=15) == 0, 'HP transfer test failed (see log)'
        reader.join(timeout=5)
        for log in logs: log.flush()
        for role in ['mac', 'hp']:
            log = (artifacts / f'transfer-{role}.log').read_text()
            assert f'PASS {role} pairing' in log
            assert f'PASS {role} round 0' in log and f'PASS {role} round 1' in log
        local_config = json.loads((artifacts / 'config/devices.json').read_text())
        remote_saved = json.loads(ssh('cat ' + shlex.quote(remote_config + '/devices.json'), capture_output=True).stdout)
        assert local_config['peers'][0]['id'] == remote_saved['id']
        assert remote_saved['peers'][0]['id'] == local_config['id']
        assert local_config['peers'][0]['key'] == remote_saved['peers'][0]['key']
        del local_config, remote_saved
        print('PASS: Mac ↔ HP automatic discovery and persistent pairing', flush=True)
        result = {'result': 'PASS', 'hosts': ['macOS', args.host], 'pairing': 'mDNS, six-digit SPAKE2 pairing, persisted both sides', 'transport': ['bidirectional UTF-8 text', '2 MiB file', 'nested Unicode directory', 'empty file', 'PNG', 'disconnect and automatic reconnect'], 'clipboard': 'synthetic; physical clipboard untouched', 'remote_config': remote_config}
        (artifacts / 'result.json').write_text(json.dumps(result, ensure_ascii=False, indent=2) + '\n')
        print('PASS: bidirectional text, image, file, directory and automatic reconnect', flush=True)
    finally:
        for process in processes:
            if process.poll() is None: process.terminate()
        for process in processes:
            try: process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)
        if reader is not None: reader.join(timeout=5)
        for log in logs: log.close()
        print('Evidence:', artifacts, flush=True)


if __name__ == '__main__':
    main()
