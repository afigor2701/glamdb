"""Real sockets, native storage and fresh-process recovery; no mock backend."""
import hashlib
import http.client
import json
import os
from pathlib import Path
import select
import socket
import subprocess
import sys
import tempfile
import unittest
import uuid
from urllib.parse import quote

SERVER, CLI, UI = sys.argv[1:4]
del sys.argv[1:4]
TOKEN = 'native-test-secret'


class NativeHTTP(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = str(Path(self.tmp.name) / 'server')
        self.store_id = str(uuid.uuid4())
        self.env = dict(os.environ, GLAMDB_TOKEN=TOKEN)
        self.assertTrue(Path(SERVER).is_file(), 'native server executable is missing')
        self.root = str(Path(self.data_dir) / 'databases' / self.store_id)
        self.start()
        self.addCleanup(self.stop)
        self.assertEqual(self.api('/databases')['items'], [])
        self.assertTrue(self.api('/capabilities')['features']['database_management'])
        self.api('/databases', {'database_id': self.store_id, 'name': 'test_database'})

    def start(self):
        self.proc = subprocess.Popen([SERVER, '--data-dir', self.data_dir,
                                      '--port', '0', '--ui-root', UI, '--request-timeout-ms', '1000'],
                                     env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        readable, _, _ = select.select([self.proc.stdout], [], [], 15)
        self.assertTrue(readable, 'server did not report listening address')
        line = self.proc.stdout.readline()
        if not line:
            self.fail(self.proc.stderr.read())
        self.port = json.loads(line)['port']

    def stop(self):
        if self.proc.poll() is None:
            self.proc.terminate()
        out, err = self.proc.communicate(timeout=20)
        self.assertEqual(self.proc.returncode, 0, out + err)

    def request(self, path, body=None, token=TOKEN, accept='application/json', method=None):
        if path.startswith(('/collections', '/%63ollections')):
            path = '/databases/' + self.store_id + path
        # Normal API calls may use the core's 60-second append deadline.
        # Leave room for transport overhead, including instrumented builds;
        # explicit framing/deadline tests keep their separate short budgets.
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=65)
        headers = {'Accept': accept}
        if token is not None:
            headers['Authorization'] = 'Bearer ' + token
        if body is not None:
            headers['Content-Type'] = 'application/json'
        conn.request(method or ('POST' if body is not None else 'GET'), path,
                     None if body is None else json.dumps(body), headers)
        response = conn.getresponse()
        data = response.read()
        status, received = response.status, dict(response.getheaders())
        conn.close()
        return status, received, data

    def api(self, path, body=None, status=200):
        code, _, data = self.request(path, body)
        self.assertEqual(code, status, data)
        return json.loads(data)

    def collection(self):
        cid = str(uuid.uuid4())
        body = {'collection_id': cid, 'name': 'events', 'schema': {'fields': [
            {'name': 'key', 'type': 'string', 'nullable': False},
            {'name': 'n', 'type': 'int64', 'nullable': True},
            {'name': 'ts', 'type': 'timestamp_us_utc', 'nullable': True}]}}
        info = self.api('/collections', body)
        self.assertEqual(info['collection_id'], cid)
        self.assertEqual(self.api('/collections', body), info)
        return '/collections/' + cid

    def test_store_readiness_is_distinct_from_process_liveness(self):
        base = '/databases/' + self.store_id
        health = self.api(base + '/readyz')
        self.assertEqual(health['state'], 'ready')
        self.assertTrue(health['read_available'])
        self.assertTrue(health['write_ready'])
        self.assertFalse(health['storage_failure_latched'])
        self.assertFalse(health['durability_qualified'])
        self.assertEqual(self.api(base + '/capabilities')['health'], health)
        self.assertIsNone(self.api('/capabilities')['health'])
        self.assertEqual(self.request(base + '/readyz', token=None)[0], 401)
        self.api(base + '/close', {})
        closed = self.api(base + '/readyz', status=503)
        self.assertEqual(closed['state'], 'closed')
        self.assertFalse(closed['read_available'])
        self.assertFalse(closed['write_ready'])
        self.assertEqual(self.api('/healthz')['status'], 'live')
        self.api(base + '/open', {})
        self.assertEqual(self.api(base + '/readyz')['state'], 'ready')

    def test_database_lifecycle_and_isolation(self):
        first = self.api('/databases/' + self.store_id)
        self.assertEqual(first['state'], 'open')
        self.assertEqual(self.api('/databases', {'name': 'test_database'}), first)
        self.api('/databases', {'name': 'different', 'database_id': self.store_id}, status=409)
        self.api('/databases', {'name': 'test_database', 'database_id': str(uuid.uuid4())}, status=409)
        second = self.api('/databases', {'name': 'second'})
        self.assertNotEqual(second['database_id'], self.store_id)
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'isolated'}]})
        other_path = '/databases/' + second['database_id']
        self.assertEqual(self.api(other_path + '/collections')['items'], [])
        self.api(other_path + path, status=404)
        same_id = self.api(path)
        self.api(other_path + '/collections', {'collection_id': same_id['collection_id'],
                 'name': same_id['name'], 'schema': same_id['schema']})
        self.api(other_path + path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'second_only'}]})
        self.assertEqual(self.api(other_path + path + '/scan', {})['rows'][0]['key'], 'second_only')
        base = '/databases/' + self.store_id
        self.assertEqual(self.api(base + '/close', {})['state'], 'closed')
        self.api(path + '/scan', {}, status=409)
        self.assertEqual(self.api(other_path)['state'], 'open')
        self.stop()
        self.start()
        self.assertEqual(self.api(base)['state'], 'closed')
        self.assertEqual(self.api(other_path)['state'], 'open')
        self.api(base + '/open', {})
        self.assertEqual(self.api(path + '/scan', {})['rows'][0]['key'], 'isolated')
        self.api(base + '/reconcile', {})
        self.assertEqual(len(self.api('/databases')['items']), 2)
        for invalid in ('', '../outside', 'a/b'):
            self.api('/databases', {'name': invalid}, status=400)
        self.api('/databases', {'name': 'x', 'path': '/tmp/outside'}, status=400)
        self.assertEqual(self.request('/databases', token=None)[0], 401)
        self.assertEqual(self.request(base + '/open', {}, token=None)[0], 401)

    def test_process_crash_reopens_registered_databases(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'survives_process_crash'}]}
        outcome = self.api(path + '/append', body)
        self.proc.kill()
        self.proc.communicate(timeout=10)
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), outcome)
        self.assertEqual(self.api(path + '/scan', {})['rows'][0]['key'], 'survives_process_crash')

    def test_observed_corruption_blocks_receipts_until_reconcile(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'recoverable'}]}
        committed = self.api(path + '/append', body)
        metadata = list(Path(self.root).rglob('*.metadata.json'))
        self.assertTrue(metadata)
        for index, file in enumerate(metadata):
            file.rename(Path(self.tmp.name) / f'metadata-{index}')
        try:
            self.assertEqual(self.api(path + '/append', body), committed)
            self.assertEqual(self.api(path + '/scan', {}, status=500)['error']['code'], 'CORRUPTION')
            unknown = self.api(path + '/append', body, status=503)
            self.assertEqual(unknown['outcome'], 'commit_unknown')
            health = self.api('/databases/' + self.store_id + '/readyz', status=503)
            self.assertEqual(health['state'], 'recovery_required')
            self.assertFalse(health['read_available'])
            self.assertFalse(health['write_ready'])
            lookup = path + '/operations/' + body['operation_id']
            self.assertEqual(self.api(lookup, status=503)['error']['code'], 'RECOVERY_REQUIRED')
            status, _, _ = self.request('/databases/' + self.store_id + '/reconcile', {})
            self.assertGreaterEqual(status, 400)
            self.assertEqual(self.api(lookup, status=503)['error']['code'], 'RECOVERY_REQUIRED')
            self.api('/databases/' + self.store_id + '/open', {}, status=503)
            self.assertEqual(self.api(lookup, status=503)['error']['code'], 'RECOVERY_REQUIRED')
            self.api('/databases', {'name': 'test_database'}, status=503)
            self.assertEqual(self.api(lookup, status=503)['error']['code'], 'RECOVERY_REQUIRED')
        finally:
            for index, file in enumerate(metadata):
                (Path(self.tmp.name) / f'metadata-{index}').rename(file)
        self.api('/databases/' + self.store_id + '/reconcile', {})
        self.assertEqual(self.api(lookup), committed)

    def test_missing_database_is_not_recreated(self):
        base = '/databases/' + self.store_id
        self.api(base + '/close', {})
        Path(self.root).rename(self.root + '.saved')
        self.api(base + '/open', {}, status=409)
        self.assertEqual(self.api(base)['state'], 'error')
        self.api('/databases', {'name': 'test_database'}, status=409)
        self.assertFalse(Path(self.root).exists())
        Path(self.root + '.saved').rename(self.root)
        self.assertEqual(self.api(base + '/open', {})['state'], 'open')

    def test_append_scan_retry_restart_and_cli(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [
            {'key': 'same', 'n': '9223372036854775807', 'ts': '2026-01-01T03:00:00.123456+03:00'},
            {'key': 'same', 'n': '-9223372036854775808', 'ts': None}]}
        receipt = self.api(path + '/append', body)
        self.assertEqual(receipt['outcome'], 'committed')
        self.assertEqual(receipt['result']['rows_appended'], '2')
        snap = receipt['result']['snapshot_id']
        self.assertEqual(self.api(path + '/append', body), receipt)
        result = self.api(path + '/scan', {})
        self.assertEqual(result['rows'], [dict(body['rows'][0], ts='2026-01-01T00:00:00.123456Z'), body['rows'][1]])
        self.assertEqual(result['snapshot_id'], snap)
        self.assertEqual(self.api(path + '/scan', {'snapshot_id': 'initial'})['rows'], [])
        self.assertEqual(self.api(path + '/scan', {'limit': 0})['rows'], [])
        filtered = self.api(path + '/scan', {'projection': ['key'], 'predicate': {'op': 'gt', 'field': 'n', 'value': '0'}})
        self.assertEqual(filtered['rows'], [{'key': 'same'}])
        self.api(path + '/append', dict(body, rows=[{'key': 'changed'}]), status=409)
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), receipt)
        self.assertEqual(self.api(path + '/scan', {'snapshot_id': snap})['rows'], result['rows'])
        self.assertTrue(list(Path(self.root).rglob('*.parquet')))
        cli = subprocess.run([CLI, 'request', '--port', str(self.port), '--path', '/databases/' + self.store_id + path + '/schema'],
                             env=self.env, check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(cli.stdout)['fields'][1]['type'], 'int64')

    def test_auth_limits_framing_and_static(self):
        for path in ('/collections', '/collections:resolve?name=events', '/capabilities', '/%63ollections'):
            self.assertEqual(self.request(path, token='wrong')[0], 401)
            self.assertEqual(self.request(path, token=None)[0], 401)
        cid = str(uuid.uuid4())
        collection = '/collections/' + cid
        for path, body in [('/databases', {}), ('/databases/' + self.store_id, None),
                           *[('/databases/' + self.store_id + '/' + action, {}) for action in ('open', 'close', 'reconcile', 'discard')],
                           ('/collections', {}), (collection, None), (collection + '/schema', None),
                           (collection + '/append', {}), (collection + '/scan', {}),
                           (collection + '/scan/open', {}), (collection + '/scan/cancel', {}),
                           (collection + '/operations/' + str(uuid.uuid4()), None),
                           *[(collection + '/' + action, None) for action in ('stats', 'snapshots', 'files')],
                           (collection + '/explain', {})]:
            with self.subTest(unauthenticated=path):
                self.assertEqual(self.request(path, body, token=None)[0], 401)
        self.assertEqual(self.request('/v0/collections')[0], 404)
        self.assertEqual(self.request('/')[0], 200)
        self.assertEqual(self.request('/workbench/config')[0], 200)
        self.assertEqual(self.request('/../store.identity')[0], 404)
        self.assertEqual(self.request('/config/demo-data.json')[0], 404)
        for headers, status in [
            ('Content-Length: 2\r\nContent-Length: 2\r\n', 400),
            ('Content-Length: 2\r\nTransfer-Encoding: chunked\r\n', 400),
            ('Content-Length: 999999999\r\n', 413),
            ('Content-Length: -1\r\n', 400),
        ]:
            with self.subTest(headers=headers), socket.create_connection(('127.0.0.1', self.port)) as sock:
                sock.sendall(('POST /databases/' + self.store_id + '/collections HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer ' + TOKEN + '\r\n' + headers + '\r\n').encode())
                self.assertIn(f' {status} '.encode(), sock.recv(4096))
        with socket.create_connection(('127.0.0.1', self.port)) as slow:
            slow.settimeout(4)
            slow.sendall(b'GET /healthz HTTP/1.1\r\n')
            self.assertIn(b' 408 ', slow.recv(4096))
        self.assertEqual(self.request('/healthz')[0], 200)

    def test_validation_capabilities_and_arrow(self):
        caps = self.api('/capabilities')
        self.assertFalse(caps['mock'])
        self.assertEqual(caps['backend'], 'local')
        self.assertEqual(
            caps['predicate_ops'],
            ['true', 'eq', 'ne', 'lt', 'le', 'gt', 'ge', 'in', 'is_null', 'is_not_null', 'and', 'or', 'not'],
        )
        self.assertEqual(caps['limits']['predicate_depth'], 64)
        self.assertEqual(caps['limits']['predicate_nodes'], 1024)
        self.assertEqual(caps['limits']['predicate_literals'], 4096)
        path = self.collection()
        for value in [1, True, '9223372036854775808']:
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'a', 'n': value}]}, status=400)
        self.assertEqual(self.api(path + '/scan', {'predicate': {'op': 'is_null', 'field': 'n'}})['rows'], [])
        self.api(path + '/scan', {'predicate': {'op': 'and', 'args': []}}, status=400)
        self.api(path + '/scan', {'predicate': {'op': 'xor', 'args': [{'op': 'true'}]}}, status=501)
        self.api(path + '/snapshots', status=501)
        self.api(path + '/files', status=501)
        self.api(path + '/stats', status=501)
        self.api(path + '/explain', {}, status=501)
        status, headers, data = self.request(path + '/scan', {}, accept='application/vnd.apache.arrow.stream')
        self.assertEqual(status, 200, data)
        self.assertEqual(headers['X-Content-SHA256'], hashlib.sha256(data).hexdigest())
        self.assertEqual(headers['X-GlamDB-Snapshot-Id'], 'initial')
        self.assertTrue(data.endswith(b'\xff\xff\xff\xff\x00\x00\x00\x00'))
        self.api(path + '/operations/' + str(uuid.uuid4()), status=404)

    def test_exact_names_round_trip_and_utf8_byte_boundaries(self):
        for name in ('line\nbreak', 'tab\tname', 'delete\x7fname', 'a+b&c=d/%', 'я' * 64):
            with self.subTest(name=name):
                cid = str(uuid.uuid4())
                created = self.api('/collections', {'collection_id': cid, 'name': name,
                    'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
                self.assertEqual(self.api('/collections:resolve?name=' + quote(name, safe='')), created)
        self.api('/collections', {'collection_id': str(uuid.uuid4()), 'name': 'я' * 64 + 'x',
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}, status=400)
        self.api('/databases', {'name': 'я' * 64})
        self.api('/databases', {'name': 'я' * 64 + 'x'}, status=400)
        for name in ('a\0b', 'a/b', 'a\\b'):
            self.api('/databases', {'name': name}, status=400)
        for path in ('/collections%0a', '/collections:resolve?na%09me=x', '/collections:resolve?name=x%00y'):
            self.api(path, status=400)

    def test_input_rejections_do_not_bind_append_ids(self):
        cid = str(uuid.uuid4())
        path = '/collections/' + cid
        schema = {'fields': [{'name': 'key', 'type': 'string'},
            {'name': 'n', 'type': 'int64', 'nullable': True},
            {'name': 'b', 'type': 'bool', 'nullable': True},
            {'name': 'f', 'type': 'float64', 'nullable': True},
            {'name': 't', 'type': 'timestamp_us_utc', 'nullable': True}]}
        self.api('/collections', {'collection_id': cid, 'name': 'typed', 'schema': schema})
        invalid_rows = [[], [None], [{}], [{'key': None}], [{'key': True}],
            [{'key': 'x', 'extra': 1}], [{'key': 'x', 'n': 1}], [{'key': 'x', 'n': True}],
            [{'key': 'x', 'n': '-9223372036854775809'}], [{'key': 'x', 'b': 1}],
            [{'key': 'x', 'f': True}], [{'key': 'x', 'f': '1.5'}],
            [{'key': 'x', 't': '2026-02-30T00:00:00Z'}], [{'key': 'x', 't': '0001-01-01T00:00:00+01:00'}],
            [{'key': 'x' * 1025}], [{'key': 'x'}] * 10001]
        for rows in invalid_rows:
            operation = str(uuid.uuid4())
            with self.subTest(rows=str(rows)[:90]):
                self.api(path + '/append', {'operation_id': operation, 'rows': rows}, status=400)
                self.api(path + '/operations/' + operation, status=404)
        operation = str(uuid.uuid4())
        for body in ({'rows': [{'key': 'x'}]}, {'operation_id': operation},
                     {'operation_id': operation, 'rows': [{'key': 'x'}], 'unknown': 1},
                     {'operation_id': False, 'rows': [{'key': 'x'}]},
                     {'operation_id': '../bad', 'rows': [{'key': 'x'}]}):
            self.api(path + '/append', body, status=400)
        self.api(path + '/operations/' + operation, status=404)
        rows = [{'key': 'я' * 512, 'n': '-9223372036854775808', 'b': False, 'f': 1.5,
                 't': '0001-01-01T00:00:00Z'}, {'key': 'nullable'}]
        self.api(path + '/append', {'operation_id': operation, 'rows': rows})
        scanned = self.api(path + '/scan', {})['rows']
        self.assertEqual(scanned[0], dict(rows[0], t='0001-01-01T00:00:00.000000Z'))
        self.assertEqual(scanned[1], {'key': 'nullable', 'n': None, 'b': None, 'f': None, 't': None})

    def test_schema_and_database_input_matrix(self):
        for body in ({}, {'name': None}, {'name': False}, {'name': 'x', 'database_id': 'bad'},
                     {'name': 'x', 'database_id': None}, {'name': 'x', 'unknown': 1}):
            self.api('/databases', body, status=400)
        for action in ('open', 'close', 'reconcile'):
            self.api('/databases/' + self.store_id + '/' + action, {'unexpected': 1}, status=400)
        for database in ('bad', str(uuid.uuid4())):
            self.api('/databases/' + database, status=400 if database == 'bad' else 404)
        base = {'collection_id': str(uuid.uuid4()), 'name': 'invalid-schema'}
        for fields in ([], [{'name': 'key', 'type': 'integer'}],
                       [{'name': 'key', 'type': 'string', 'nullable': True}],
                       [{'name': 'key', 'type': 'int64'}],
                       [{'name': 'key', 'type': 'string', 'field_id': 2}],
                       [{'name': 'key', 'type': 'string', 'field_id': True}],
                       [{'name': 'key', 'type': 'string', 'nullable': 'false'}],
                       [{'name': 'key', 'type': 'string'}] * 2,
                       [{'name': 'key', 'type': 'string'}, {'name': 'я' * 65, 'type': 'string'}],
                       [{'name': 'key', 'type': 'string'}] + [{'name': f'n{i}', 'type': 'int64'} for i in range(64)]):
            with self.subTest(fields=fields):
                self.api('/collections', dict(base, schema={'fields': fields}), status=400)
        self.assertEqual(self.api('/collections')['items'], [])
        valid = dict(base, schema={'fields': [{'name': 'key', 'type': 'string', 'field_id': 1}]})
        for body in ({}, dict(valid, collection_id='bad'), dict(valid, name=False), dict(valid, unexpected=1)):
            self.api('/collections', body, status=400)
        self.api('/collections', dict(valid, mode='upsert'), status=501)
        self.api('/collections', valid)

    def test_preconditions_and_operation_ids_are_durable_and_isolated(self):
        path = self.collection()
        operation = str(uuid.uuid4())
        body = {'operation_id': operation, 'rows': [{'key': 'first'}], 'expected_snapshot': None}
        first = self.api(path + '/append', body)
        rejected = dict(body, operation_id=str(uuid.uuid4()), rows=[{'key': 'rejected'}])
        failure = self.api(path + '/append', rejected, status=412)
        self.assertEqual(failure['outcome'], 'definitely_not_committed')
        self.assertEqual(self.api(path + '/operations/' + rejected['operation_id'], status=412), failure)
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'next'}],
            'expected_snapshot': first['result']['snapshot_id']})
        self.assertEqual(self.api(path + '/append', body), first)

        other = self.api('/databases', {'name': 'other'})['database_id']
        cid = path.rsplit('/', 1)[-1]
        other_path = '/databases/' + other + path
        self.api('/databases/' + other + '/collections', {'collection_id': cid, 'name': 'events',
            'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
        self.api(other_path + '/operations/' + operation, status=404)
        self.api(other_path + '/append', {'operation_id': operation, 'rows': [{'key': 'other'}]})
        self.assertEqual(self.api(other_path + '/scan', {})['rows'], [{'key': 'other'}])
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/append', rejected, status=412), failure)
        self.assertEqual(self.api(path + '/append', body), first)

    def test_append_row_and_string_byte_boundaries(self):
        cid = str(uuid.uuid4())
        path = '/collections/' + cid
        self.api('/collections', {'collection_id': cid, 'name': 'byte-limits', 'schema': {'fields': [
            {'name': 'key', 'type': 'string'}, {'name': 'a', 'type': 'string', 'nullable': True},
            {'name': 'b', 'type': 'string', 'nullable': True}]}})
        # Canonical row bytes: three null tags + three 4-byte lengths + key byte.
        exact = {'key': 'x', 'a': 'a' * 1048576, 'b': 'b' * 1048560}
        operation = str(uuid.uuid4())
        self.api(path + '/append', {'operation_id': operation, 'rows': [exact]})
        self.assertEqual(self.api(path + '/scan', {})['rows'], [exact])
        for row, status in ((dict(exact, b=exact['b'] + 'b'), 413),
                            ({'key': 'x', 'a': 'a' * 1048577}, 400)):
            rejected = str(uuid.uuid4())
            self.api(path + '/append', {'operation_id': rejected, 'rows': [row]}, status=status)
            self.api(path + '/operations/' + rejected, status=404)
        rows_path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'duplicate'}] * 10000}
        result = self.api(rows_path + '/append', body)
        self.assertEqual(result['result']['rows_appended'], '10000')
        rows = self.api(rows_path + '/scan', {})['rows']
        self.assertEqual(len(rows), 10000)
        self.assertTrue(all(row['key'] == 'duplicate' for row in rows))

    def test_raw_json_and_response_diagnostics_are_bounded(self):
        for wire in (b'{', b'[]', b'{"name":"a","name":"b"}', b'{"name":"\xff"}',
                     b'{"name":' + b'[' * 161 + b'0' + b']' * 161 + b'}'):
            conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=10)
            conn.request('POST', '/databases', wire,
                         {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
            response = conn.getresponse()
            self.assertEqual(response.status, 400, response.read())
            conn.close()
        wire = b'{"' + b'x' * (16 * 1024 * 1024 - 7) + b'":0}'
        conn = http.client.HTTPConnection('127.0.0.1', self.port, timeout=30)
        conn.request('POST', '/databases', wire,
                     {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        response = conn.getresponse()
        data = response.read()
        self.assertEqual(response.status, 400)
        self.assertLess(len(data), 16384)
        self.assertEqual(json.loads(data)['error']['code'], 'INVALID_ARGUMENT')
        conn.close()

    def test_concurrent_appends_and_idle_shutdown(self):
        from concurrent.futures import ThreadPoolExecutor
        path = self.collection()
        requests = [{'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(i)}]} for i in range(8)]
        with ThreadPoolExecutor(max_workers=8) as workers:
            responses = list(workers.map(lambda body: self.request(path + '/append', body), requests))
        outcomes = []
        for body, (status, _, data) in zip(requests, responses):
            if status == 503:
                self.assertEqual(json.loads(data)['error']['code'], 'SERVER_BUSY')
                # Explicitly retry admission backpressure with the original ID.
                outcomes.append(self.api(path + '/append', body))
            else:
                self.assertEqual(status, 200, data)
                outcomes.append(json.loads(data))
        self.assertEqual(len({item['result']['snapshot_id'] for item in outcomes}), 8)
        rows = self.api(path + '/scan', {})['rows']
        self.assertEqual(sorted(row['key'] for row in rows), list(map(str, range(8))))
        with socket.create_connection(('127.0.0.1', self.port)) as idle:
            idle.settimeout(5)
            idle.sendall(b'GET /healthz HTTP/1.1\r\n')
            self.stop()
            self.assertEqual(idle.recv(4096), b'')
        self.start()
        for request, outcome in zip(requests, outcomes):
            self.assertEqual(self.api(path + '/operations/' + request['operation_id']), outcome)

    def test_route_schema_fixtures(self):
        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.json').read_text())
        fixture['database_id'] = self.store_id
        kinds = {'object': dict, 'array': list, 'string': str, 'boolean': bool, 'integer': int, 'null': type(None)}
        wire_schema = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.schema.json').read_text())
        validator = None
        if os.environ.get('GLAMDB_VALIDATE_SCHEMAS') == '1':
            from jsonschema import Draft202012Validator
            Draft202012Validator.check_schema(wire_schema)
            validator = Draft202012Validator
            error = {'code': 'IO_ERROR', 'message': 'lost commit', 'retry_hint': 'SameIdAfterReconcile'}
            create_validator = validator(dict(wire_schema, **{'$ref': '#/$defs/createOutcome'}))
            definite = {'outcome': 'definitely_not_committed', 'phase': 'validation', 'error': error}
            create_validator.validate(definite)
            for invalid in (
                {'outcome': 'commit_unknown', 'phase': 'catalog_commit', 'error': error},
                dict(definite, phase='complete'),
                dict(definite, recovery_required=True),
            ):
                self.assertFalse(create_validator.is_valid(invalid), invalid)
            health_validator = validator(dict(wire_schema, **{'$ref': '#/$defs/storeHealth'}))
            health_fixtures = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/health-states.json').read_text())
            for health_fixture in health_fixtures:
                health_validator.validate(health_fixture['response'])
                invalid = dict(health_fixture['response'], durability_qualified=True)
                self.assertFalse(health_validator.is_valid(invalid))
            outcomes = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
            cases = {case['name']: case for case in outcomes['cases']}
            for case in cases.values():
                kind = 'createOutcome' if case['kind'] == 'create' else 'appendOutcome'
                with self.subTest(outcome_fixture=case['name']):
                    validator(dict(wire_schema, **{'$ref': '#/$defs/' + kind})).validate(case['response'])
            for invalid in outcomes['invalid_recovery']:
                if 'remove' not in invalid:
                    continue
                case = cases[invalid['case']]
                response = json.loads(json.dumps(case['response']))
                parts = invalid['remove'].strip('/').split('/')
                target = response
                for part in parts[:-1]:
                    target = target[part]
                del target[parts[-1]]
                kind = 'createOutcome' if case['kind'] == 'create' else 'appendOutcome'
                with self.subTest(outcome_fixture=invalid['name']):
                    self.assertFalse(validator(dict(wire_schema, **{'$ref': '#/$defs/' + kind})).is_valid(response))
            identifier = fixture['collection_id']
            recovery = {
                'store_id': self.store_id, 'collection_id': identifier,
                'operation_id': fixture['operation_id'], 'payload_hash': '0' * 64,
                'hash_version': 1, 'attempt_id': identifier,
                'base': {'store_id': self.store_id, 'collection_id': identifier,
                         'table_uuid': identifier, 'metadata_location': 'store://metadata',
                         'current_snapshot_id': None, 'schema_id': 0, 'spec_id': 0},
                'candidate_snapshot_id': None, 'candidate_metadata_location': None,
                'first_error': error, 'publication_started': True, 'lookup_blocked': False,
            }
            unknown = {'outcome': 'commit_unknown', 'phase': 'catalog_commit', 'error': error,
                       'operation_id': fixture['operation_id'], 'recovery_required': True, 'recovery': recovery}
            append_validator = validator(dict(wire_schema, **{'$ref': '#/$defs/appendOutcome'}))
            append_validator.validate(unknown)
            for required in ('error', 'recovery', 'recovery_required'):
                invalid = dict(unknown)
                del invalid[required]
                self.assertFalse(append_validator.is_valid(invalid), invalid)
            create_unknown = {key: value for key, value in unknown.items() if key != 'operation_id'}
            create_unknown['recovery'] = {
                'store_id': self.store_id, 'collection_id': identifier, 'exact_name': 'events',
                'schema_hash': '0' * 64, 'attempt_id': identifier, 'candidate_table_uuid': identifier,
                'candidate_metadata_location': None, 'publication_started': True,
            }
            create_validator.validate(create_unknown)
            for required in ('error', 'recovery', 'recovery_required'):
                invalid = dict(create_unknown)
                del invalid[required]
                self.assertFalse(create_validator.is_valid(invalid), invalid)

        for route in fixture['routes']:
            path = route['path'].format(**fixture)
            request = route.get('request')
            if request and isinstance(request.get('cursor'), str):
                request = dict(request, cursor=request['cursor'].format(**fixture))
            with self.subTest(path=path, method=route['method']):
                if validator and 'request_schema' in route:
                    validator(dict(wire_schema, **{'$ref': '#/$defs/' + route['request_schema']})).validate(request)
                status, headers, data = self.request(path, request, method=route['method'], accept=route.get('accept', 'application/json'))
                self.assertEqual(status, route['status'], data)
                if route.get('accept'):
                    self.assertEqual(headers['Content-Type'], route['accept'])
                    self.assertEqual(headers['X-Content-SHA256'], hashlib.sha256(data).hexdigest())
                    continue
                value = json.loads(data)
                for variable, field in route.get('capture', {}).items():
                    fixture[variable] = value[field]
                if validator:
                    validator(dict(wire_schema, **{'$ref': '#/$defs/' + route['response_schema']})).validate(value)
                for name, kind in route['response_fields'].items():
                    self.assertIn(name, value)
                    self.assertIn(type(value[name]), tuple(kinds[item] for item in kind.split('|')))

    def test_compound_predicates_and_oracle_equivalence(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'tools/verification/src'))
        from glamdb_verify import scan_rows, wire_rows

        cid = str(uuid.uuid4())
        schema = {
            'fields': [
                {'name': 'key', 'type': 'string', 'nullable': False},
                {'name': 'id', 'type': 'int64', 'nullable': False},
                {'name': 'a', 'type': 'bool', 'nullable': True},
                {'name': 'b', 'type': 'bool', 'nullable': True},
                {'name': 'n', 'type': 'int64', 'nullable': True},
                {'name': 'f', 'type': 'float64', 'nullable': True},
                {'name': 's', 'type': 'string', 'nullable': True},
            ]
        }
        self.api('/collections', {'collection_id': cid, 'name': 'oracle_3vl', 'schema': schema})
        path = '/collections/' + cid
        tri = [True, None, False]
        ns = [None, 1, 2, 2, -5, None, 1, 3, 0]
        fs = [-0.0, 0.0, 1.5, None, -2.5, 0.0, None, 3.25, -0.0]
        ss = [None, 'one', 'two', 'two', 'neg', None, 'one', 'three', 'zero']
        oracle_rows = []
        for idx, (av, bv) in enumerate((a, b) for a in tri for b in tri):
            oracle_rows.append({
                'key': f'k{idx}',
                'id': idx,
                'a': av,
                'b': bv,
                'n': ns[idx],
                'f': fs[idx],
                's': ss[idx],
            })
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': wire_rows(schema, oracle_rows)})

        pred_a = {'op': 'eq', 'field': 'a', 'value': True}
        pred_b = {'op': 'eq', 'field': 'b', 'value': True}
        predicates = [
            None,
            {'op': 'true'},
            {'op': 'not', 'arg': {'op': 'true'}},
            {'op': 'and', 'args': [pred_a, pred_b]},
            {'op': 'or', 'args': [pred_a, pred_b]},
            {'op': 'not', 'arg': {'op': 'and', 'args': [pred_a, pred_b]}},
            {'op': 'or', 'args': [{'op': 'not', 'arg': pred_a}, {'op': 'not', 'arg': pred_b}]},
            {'op': 'not', 'arg': {'op': 'or', 'args': [pred_a, pred_b]}},
            {'op': 'and', 'args': [{'op': 'not', 'arg': pred_a}, {'op': 'not', 'arg': pred_b}]},
            {'op': 'in', 'field': 'n', 'values': []},
            {'op': 'not', 'arg': {'op': 'in', 'field': 'n', 'values': []}},
            {'op': 'in', 'field': 'n', 'values': ['1', None]},
            {'op': 'not', 'arg': {'op': 'in', 'field': 'n', 'values': ['1', None]}},
            {'op': 'in', 'field': 'n', 'values': ['1', '2']},
            {'op': 'not', 'arg': {'op': 'in', 'field': 'n', 'values': ['1', '2']}},
            {'op': 'eq', 'field': 'n', 'value': None},
            {'op': 'not', 'arg': {'op': 'eq', 'field': 'n', 'value': None}},
            {'op': 'is_null', 'field': 'n'},
            {'op': 'is_not_null', 'field': 'n'},
            {'op': 'eq', 'field': 'f', 'value': -0.0},
            {'op': 'in', 'field': 'f', 'values': [-0.0]},
            {
                'op': 'and',
                'args': [
                    {'op': 'true'},
                    {
                        'op': 'or',
                        'args': [
                            {'op': 'is_null', 'field': 's'},
                            {'op': 'in', 'field': 'n', 'values': ['1', '3']},
                        ],
                    },
                    {'op': 'not', 'arg': {'op': 'eq', 'field': 'b', 'value': False}},
                ],
            },
        ]
        for pred in predicates:
            with self.subTest(predicate=pred):
                expected = wire_rows(
                    {'fields': [{'name': 'key', 'type': 'string', 'nullable': False},
                                {'name': 'id', 'type': 'int64', 'nullable': False}]},
                    scan_rows(schema, oracle_rows, ['key', 'id'], pred),
                )
                actual = self.api(path + '/scan', {'projection': ['key', 'id'], 'predicate': pred})['rows']
                self.assertEqual(actual, expected)
                opened = self.api(path + '/scan/open', {'projection': ['key', 'id'], 'predicate': pred, 'batch_rows': 2})
                paged = []
                cursor = opened['cursor']
                while cursor:
                    page = self.api(path + '/scan', {'cursor': cursor})
                    paged.extend(page['rows'])
                    cursor = page['next_cursor']
                self.assertEqual(paged, expected)

        depth_64 = {'op': 'true'}
        for _ in range(64):
            depth_64 = {'op': 'and', 'args': [depth_64]}
        self.assertEqual(len(self.api(path + '/scan', {'projection': ['id'], 'predicate': depth_64})['rows']), 9)
        depth_65 = {'op': 'not', 'arg': depth_64}
        err_65 = self.api(path + '/scan', {'projection': ['id'], 'predicate': depth_65}, status=400)['error']
        self.assertEqual(err_65['code'], 'INVALID_ARGUMENT')
        self.assertEqual(err_65['retry_hint'], 'FixInput')

    def test_sdk_json_and_arrow(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
        try:
            from glamdb_sdk import Client
        except ImportError:
            if os.environ.get('GLAMDB_REQUIRE_NATIVE_CLIENTS') == '1':
                raise
            self.skipTest('SDK dependencies are tested in the workbench environment')
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.store_id) as client:
            self.assertEqual(len(client.list_databases().items), 1)
            self.assertEqual(client.get_database(self.store_id)['state'], 'open')
            self.assertEqual(client.close_database(self.store_id)['state'], 'closed')
            self.assertEqual(client.open_database(self.store_id)['state'], 'open')
            self.assertEqual(client.reconcile_database(self.store_id)['state'], 'open')
            self.assertEqual(client.create_database('test_database')['database_id'], self.store_id)
            self.assertEqual(client.get_schema(cid).fields[1].type, 'int64')
            outcome = client.append(cid, [{'key': 'sdk', 'n': 9223372036854775807}], operation_id=str(uuid.uuid4()))
            self.assertEqual(outcome.outcome, 'committed')
            self.assertEqual(client.scan_json(cid).rows[0]['n'], 9223372036854775807)
            with client.scan_batches(cid) as stream:
                import pyarrow as pa
                table = pa.Table.from_batches(list(stream), schema=stream.schema)
                self.assertEqual(table.to_pylist()[0]['n'], 9223372036854775807)

    def test_shared_outcome_fixtures_with_native_storage_and_sdk(self):
        sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
        try:
            from glamdb_sdk import Client, Schema
            from glamdb_sdk.codec import encode_rows
            import pyarrow as pa
        except ImportError:
            if os.environ.get('GLAMDB_REQUIRE_NATIVE_CLIENTS') == '1':
                raise
            self.skipTest('SDK dependencies are tested in the workbench environment')

        fixture = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/outcomes-v0.json').read_text())
        cid, schema = fixture['collection_id'], Schema.from_dict(fixture['schema'])
        path = '/collections/' + cid
        original = None
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.store_id) as client:
            created = client.create_collection(fixture['exact_name'], schema, collection_id=cid)
            self.assertEqual(created.outcome, 'committed')
            for name in fixture['retry_sequence']:
                request = fixture['appends'][name]
                prepared = client.prepare_append(cid, request['rows'], operation_id=request['operation_id'], schema=schema)
                self.assertEqual(prepared.payload_hash, request['payload_hash'])
                outcome = client.append(prepared)
                self.assertEqual(outcome.outcome, 'committed')
                self.assertEqual(outcome.result['payload_hash'], request['payload_hash'])
                self.assertEqual(outcome.result['rows_appended'], str(len(request['rows'])))
                if name == 'a':
                    if original is None:
                        original = outcome
                    else:
                        self.assertEqual(outcome, original)
                else:
                    self.assertEqual(outcome.result['parent_snapshot_id'], original.result['snapshot_id'])
                    latest = outcome.result['snapshot_id']
            self.assertCountEqual(encode_rows(schema, client.scan_json(cid).rows), fixture['expected_latest_rows'])
            self.assertCountEqual(encode_rows(schema, client.scan_json(cid, snapshot_id=original.result['snapshot_id']).rows),
                             fixture['appends']['a']['rows'])
            self.assertEqual(client.scan_json(cid, snapshot_id=None).rows, [])
            with client.scan_batches(cid) as stream:
                table = pa.Table.from_batches(list(stream), schema=stream.schema)
                self.assertCountEqual(encode_rows(schema, table.to_pylist()), fixture['expected_latest_rows'])
        self.assertEqual(self.api(path + '/scan', {})['snapshot_id'], latest)
        self.assertCountEqual(self.api(path + '/scan', {})['rows'], fixture['expected_latest_rows'])
        self.assertEqual(len(list(Path(self.root).rglob('*.parquet'))), 2)
        self.assertTrue((Path(self.root) / 'catalog.sqlite').is_file())
        self.stop()
        self.start()
        with Client(f'http://127.0.0.1:{self.port}', TOKEN, database_id=self.store_id) as client:
            request = fixture['appends']['a']
            self.assertEqual(client.find_append_outcome(cid, request['operation_id']), original)
            self.assertEqual(client.append(cid, request['rows'], operation_id=request['operation_id'], schema=schema), original)
            self.assertCountEqual(encode_rows(schema, client.scan_json(cid).rows), fixture['expected_latest_rows'])
        self.assertEqual(self.api(path + '/scan', {})['snapshot_id'], latest)
        self.assertEqual(len(list(Path(self.root).rglob('*.parquet'))), 2)

    def test_owner_invalid_json_disconnect_and_shutdown(self):
        owner = subprocess.run([SERVER, '--data-dir', self.data_dir, '--port', '0'],
                               env=self.env, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(owner.returncode, 0)
        self.assertIn('STATE_IN_USE', owner.stderr)
        second = subprocess.run([SERVER, '--root', self.root, '--store-id', self.store_id, '--port', '0'],
                                env=self.env, capture_output=True, text=True, timeout=10)
        self.assertNotEqual(second.returncode, 0)
        self.assertIn('STATE_IN_USE', second.stderr)
        self.assertEqual(self.request('/%63ollections', token='wrong')[0], 401)
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'lost-response'}]}
        wire = json.dumps(body).encode()
        with socket.create_connection(('127.0.0.1', self.port)) as sock:
            sock.sendall((f'POST /databases/{self.store_id}{path}/append HTTP/1.1\r\nHost: localhost\r\nAuthorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\nContent-Length: {len(wire)}\r\n\r\n').encode() + wire)
            # Receiving the response headers proves dispatch before discarding it.
            self.assertIn(b'200', sock.recv(16))
        self.stop()
        self.start()
        outcome = self.api(path + '/operations/' + body['operation_id'])
        self.assertEqual(outcome['outcome'], 'committed')
        self.assertEqual(self.api(path + '/append', body), outcome)


if __name__ == '__main__':
    unittest.main()
