"""Native fixed-view scan pagination, retry and lifetime contracts."""
import json
import unittest
import uuid
import sys

_test_arguments = sys.argv[4:]
from native_http_test import NativeHTTP
sys.argv[1:] = _test_arguments


class ScanHTTP(NativeHTTP):
    # Reuse only the socket/process helpers, not the unrelated inherited cases.
    def test_scan_fixed_identity(self):
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'a'}]})
        old = self.api(path + '/scan', {})['snapshot_id']
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'b'}]})
        for selector, expected in [('initial', None), (old, old)]:
            result = self.api(path + '/scan', {'snapshot_id': selector})
            self.assertEqual(result['snapshot_id'], expected)
            status, headers, _ = self.request(path + '/scan', {'snapshot_id': selector}, accept='application/vnd.apache.arrow.stream')
            self.assertEqual(status, 200)
            self.assertEqual(headers['X-GlamDB-Snapshot-Id'], expected or 'initial')
        compound = {
            'op': 'and',
            'args': [
                {'op': 'is_not_null', 'field': 'key'},
                {'op': 'or', 'args': [{'op': 'in', 'field': 'key', 'values': ['a', None]}, {'op': 'eq', 'field': 'key', 'value': 'b'}]},
                {'op': 'not', 'arg': {'op': 'eq', 'field': 'key', 'value': 'b'}},
            ],
        }
        self.assertEqual(self.api(path + '/scan', {'projection': ['key'], 'predicate': compound})['rows'], [{'key': 'a'}])

    def test_scan_duplicate_pages_replay_cancel(self):
        path = self.collection()
        for _ in range(11):
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'same'}] * 10000})
        opened = self.api(path + '/scan/open', {'batch_rows': 65536})
        request = {'cursor': opened['cursor']}
        # Lose the complete first-page body after the server committed its
        # cursor advancement. Retrying the same token must replay that page.
        import http.client
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=20)
        connection.request('POST', '/databases/' + self.store_id + path + '/scan',
                           json.dumps(request), {'Content-Type': 'application/json',
                           'Authorization': 'Bearer native-test-secret'})
        self.assertEqual(connection.getresponse().status, 200)
        connection.close()
        first = self.api(path + '/scan', request)
        self.assertEqual(first, self.api(path + '/scan', request))
        self.assertIsNotNone(first['next_cursor'])
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'later'}]})
        total = len(first['rows'])
        cursor = first['next_cursor']
        while cursor:
            page = self.api(path + '/scan', {'cursor': cursor})
            self.assertEqual(page, self.api(path + '/scan', {'cursor': cursor}))
            self.assertEqual(page['snapshot_id'], first['snapshot_id'])
            self.assertTrue(all(row['key'] == 'same' for row in page['rows']))
            total += len(page['rows'])
            cursor = page['next_cursor']
        self.assertEqual(total, 110000)
        self.api(path + '/scan', {'cursor': first['next_cursor']}, status=404)
        pending = self.api(path + '/scan', {})['next_cursor']
        self.api(path + '/scan/cancel', {'cursor': pending})
        self.api(path + '/scan', {'cursor': pending}, status=404)
        pending = self.api(path + '/scan', {})['next_cursor']
        self.api('/databases/' + self.store_id + '/close', {})
        self.api('/databases/' + self.store_id + '/open', {})
        self.api(path + '/scan', {'cursor': pending}, status=404)

    def test_scan_large_payload_arrow_sdk_and_foreign_cursor(self):
        import hashlib
        import pyarrow as pa
        from glamdb_sdk import Client
        cid = str(uuid.uuid4())
        schema = {'fields': [{'name': 'key', 'type': 'string', 'nullable': False},
                             {'name': 'payload', 'type': 'string', 'nullable': True}]}
        self.api('/collections', {'collection_id': cid, 'name': 'large', 'schema': schema})
        path = '/collections/' + cid
        rows = [{'key': 'same', 'payload': 'x' * 100000}] * 90
        for _ in range(2):
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
        with Client('http://127.0.0.1:' + str(self.port), 'native-test-secret', database_id=self.store_id) as client:
            self.assertEqual(sum(len(page.rows) for page in client.scan_pages(cid)), 180)
            self.assertEqual(len(client.scan_json(cid).rows), 180)
            with client.scan_batches(cid) as scan:
                self.assertEqual(sum(batch.num_rows for batch in scan), 180)
                self.assertEqual(scan.integrity, 'verified')
        opened_status, _, opened_bytes = self.request(path + '/scan/open', {'projection': ['payload', 'key']},
                                                       accept='application/vnd.apache.arrow.stream')
        self.assertEqual(opened_status, 200)
        cursor = json.loads(opened_bytes)['cursor']
        self.api(path + '/scan', {'cursor': cursor}, status=400)  # format bound
        other = self.api('/databases', {'name': 'foreign'})['database_id']
        self.api('/databases/' + other + '/collections', {'collection_id': cid, 'name': 'large', 'schema': schema})
        self.api('/databases/' + other + path + '/scan', {'cursor': cursor}, status=404)
        count = 0
        while cursor:
            status, headers, data = self.request(path + '/scan', {'cursor': cursor}, accept='application/vnd.apache.arrow.stream')
            self.assertEqual(status, 200, data)
            self.assertEqual(hashlib.sha256(data).hexdigest(), headers['X-Content-SHA256'])
            self.assertEqual(self.request(path + '/scan', {'cursor': cursor}, accept='application/vnd.apache.arrow.stream')[2], data)
            table = pa.ipc.open_stream(data).read_all()
            self.assertEqual(table.schema.names, ['payload', 'key'])
            count += table.num_rows
            cursor = headers.get('X-GlamDB-Next-Cursor')
        self.assertEqual(count, 180)

    def test_single_row_above_page_target_is_not_rejected(self):
        cid = str(uuid.uuid4())
        fields = [{'name': 'key', 'type': 'string', 'nullable': False}] + [
            {'name': 's' + str(i), 'type': 'string', 'nullable': True} for i in range(9)]
        self.api('/collections', {'collection_id': cid, 'name': 'wide', 'schema': {'fields': fields}})
        path = '/collections/' + cid
        row = dict({'key': 'wide'}, **{'s' + str(i): '\x01' * 175000 for i in range(9)})
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [row]})
        result = self.api(path + '/scan', {})
        self.assertEqual(result['rows'], [row])
        self.assertIsNone(result['next_cursor'])
        import pyarrow as pa
        status, _, data = self.request(path + '/scan', {}, accept='application/vnd.apache.arrow.stream')
        self.assertEqual(status, 200)
        self.assertEqual(pa.ipc.open_stream(data).read_all().to_pylist(), [row])

    def test_replay_pressure_preserves_explicit_cursor(self):
        fields = [{'name': 'key', 'type': 'string', 'nullable': False}] + [
            {'name': 's' + str(i), 'type': 'string', 'nullable': True} for i in range(9)]
        row = dict({'key': 'first'}, **{'s' + str(i): '\x01' * 175000 for i in range(9)})
        second = dict(row, key='second')

        def seed(database):
            base = '/databases/' + database + '/collections'
            collection = str(uuid.uuid4())
            self.api(base, {'collection_id': collection, 'name': 'large', 'schema': {'fields': fields}})
            path = base + '/' + collection
            for value in (row, second):
                self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [value]})
            return path

        victim = seed(self.store_id)
        opened = self.api(victim + '/scan/open', {})
        holders = [seed(self.api('/databases', {'name': 'holder' + str(i)})['database_id']) for i in range(2)]
        tokens = []
        for index in range(16):
            path = holders[index // 8]
            status, _, data = self.request(path + '/scan', {})
            page = json.loads(data)
            if status == 429:
                self.assertEqual(page['error']['code'], 'RESOURCE_EXHAUSTED')
                break
            self.assertEqual(status, 200, data)
            self.assertGreater(len(data), 9000000)
            tokens.append((path, page['next_cursor']))
        else:
            self.fail('replay capacity was not enforced')

        request = {'cursor': opened['cursor']}
        for _ in range(2):
            failure = self.api(victim + '/scan', request, status=429)
            self.assertEqual(failure['error']['code'], 'RESOURCE_EXHAUSTED')
        path, token = tokens.pop()
        self.api(path + '/scan/cancel', {'cursor': token})
        self.api(victim + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'later'}]})
        first = self.api(victim + '/scan', request)
        self.assertIn(first['rows'], ([row], [second]))
        self.assertEqual(first['snapshot_id'], opened['snapshot_id'])
        self.assertEqual(self.api(victim + '/scan', request), first)
        for path, token in tokens:
            self.api(path + '/scan/cancel', {'cursor': token})
        last = self.api(victim + '/scan', {'cursor': first['next_cursor']})
        self.assertEqual(sorted(first['rows'] + last['rows'], key=lambda value: value['key']), [row, second])
        self.assertEqual(last['snapshot_id'], opened['snapshot_id'])
        self.assertIsNone(last['next_cursor'])

    def test_open_capacity_cancel_reconcile_and_restart(self):
        path = self.collection()
        tokens = [self.api(path + '/scan/open', {})['cursor'] for _ in range(8)]
        self.api(path + '/scan/open', {}, status=429)
        self.api(path + '/scan/cancel', {'cursor': tokens.pop()})
        self.api(path + '/scan/open', {})
        self.api('/databases/' + self.store_id + '/reconcile', {})
        self.api(path + '/scan', {'cursor': tokens[0]}, status=404)
        token = self.api(path + '/scan/open', {})['cursor']
        self.stop()
        self.start()
        self.api(path + '/scan', {'cursor': token}, status=404)

    def test_scan_read_failure_never_becomes_eos(self):
        from pathlib import Path
        for after_page in (False, True):
            cid = str(uuid.uuid4())
            self.api('/collections', {'collection_id': cid, 'name': 'fault_' + str(after_page).lower(),
                'schema': {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}})
            path = '/collections/' + cid
            previous_files = set(Path(self.root).rglob('*.parquet'))
            for _ in range(3 if after_page else 1):
                self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                    'rows': [{'key': 'same'}] * (10000 if after_page else 1)})
            cursor = self.api(path + '/scan/open', {})['cursor']
            if after_page:
                first = self.api(path + '/scan', {'cursor': cursor})
                self.assertEqual(len(first['rows']), 10000)
                cursor = first['next_cursor']
            files = set(Path(self.root).rglob('*.parquet')) - previous_files
            self.assertTrue(files)
            originals = {file: file.read_bytes() for file in files}
            try:
                for file in files:
                    file.write_bytes(b'broken parquet')
                failed = False
                for _ in range(3):
                    status, _, data = self.request(path + '/scan', {'cursor': cursor})
                    if status != 200:
                        self.assertEqual(status, 500, data)
                        self.assertIn(json.loads(data)['error']['code'], ['CORRUPTION', 'IO_ERROR'])
                        self.api(path + '/scan', {'cursor': cursor}, status=503)
                        failed = True
                        break
                    cursor = json.loads(data)['next_cursor']
                    self.assertIsNotNone(cursor, 'read failure was silently converted to EOS')
                self.assertTrue(failed)
            finally:
                for file, content in originals.items():
                    file.write_bytes(content)
            self.api('/databases/' + self.store_id + '/reconcile', {})
            self.api(path + '/scan', {'cursor': cursor}, status=404)

    def test_cancel_live_cursor_while_core_readiness_is_fenced(self):
        from pathlib import Path
        healthy = self.collection()
        live = self.api(healthy + '/scan/open', {})['cursor']
        cid = str(uuid.uuid4())
        self.api('/collections', {'collection_id': cid, 'name': 'damaged',
            'schema': {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}})
        damaged = '/collections/' + cid
        self.api(damaged + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'broken'}]})
        corrupt_cursor = self.api(damaged + '/scan/open', {})['cursor']
        originals = {file: file.read_bytes() for file in Path(self.root).rglob('*.parquet')}
        self.assertTrue(originals)
        try:
            for file in originals:
                file.write_bytes(b'broken parquet')
            self.api(damaged + '/scan', {'cursor': corrupt_cursor}, status=500)
            self.api(healthy + '/scan', {'cursor': live}, status=503)
            self.api(healthy + '/scan/cancel', {'cursor': live})
            self.api(damaged + '/scan/cancel', {'cursor': live}, status=404)
            self.api(healthy + '/scan/cancel', {'cursor': live}, status=404)
        finally:
            for file, content in originals.items():
                file.write_bytes(content)

    def test_sdk_constructor_failure_releases_native_scan_capacity(self):
        from glamdb_sdk import Client, IntegrityError
        path = self.collection()
        cid = path.rsplit('/', 1)[-1]
        with Client('http://127.0.0.1:' + str(self.port), 'native-test-secret',
                    database_id=self.store_id, timeout=60) as writer:
            for count in (10000, 1):
                outcome = writer.append(cid, [{'key': 'same'}] * count, operation_id=str(uuid.uuid4()))
                self.assertEqual(outcome.outcome, 'committed')
        for _ in range(9):
            with Client('http://127.0.0.1:' + str(self.port), 'native-test-secret',
                        database_id=self.store_id, max_arrow_read_bytes=1) as client:
                with self.assertRaises(IntegrityError):
                    client.scan_batches(cid)
        # Every failed constructor must release ownership immediately, not
        # merely after the 60-second server expiry.
        tokens = [self.api(path + '/scan/open', {})['cursor'] for _ in range(8)]
        for token in tokens:
            self.api(path + '/scan/cancel', {'cursor': token})

    def test_abandoned_sdk_iterators_release_native_scan_capacity(self):
        import gc
        from glamdb_sdk import Client
        path = self.collection()
        cid = path.rsplit('/', 1)[-1]
        with Client('http://127.0.0.1:' + str(self.port), 'native-test-secret',
                    database_id=self.store_id, timeout=60) as client:
            for count in (10000, 1):
                client.append(cid, [{'key': 'same'}] * count, operation_id=str(uuid.uuid4()))
            for scan in (client.scan_pages, client.scan_batches):
                for _ in range(9):
                    for page in scan(cid):
                        break
                    gc.collect()
            # Keep the original client alive: cleanup must follow iterator lifetime.
            with Client('http://127.0.0.1:' + str(self.port), 'native-test-secret',
                        database_id=self.store_id) as other:
                self.assertEqual(len(other.scan_json(cid).rows), 10001)
        tokens = [self.api(path + '/scan/open', {})['cursor'] for _ in range(8)]
        for token in tokens:
            self.api(path + '/scan/cancel', {'cursor': token})

    def test_scan_capacity_is_isolated_between_databases(self):
        path = self.collection()
        tokens = [self.api(path + '/scan/open', {})['cursor'] for _ in range(8)]
        self.api(path + '/scan/open', {}, status=429)
        other = self.api('/databases', {'name': 'quota_other'})['database_id']
        cid = str(uuid.uuid4())
        base = '/databases/' + other + '/collections'
        self.api(base, {'collection_id': cid, 'name': 'other',
            'schema': {'fields': [{'name': 'key', 'type': 'string', 'nullable': False}]}})
        result = self.api(base + '/' + cid + '/scan', {})
        self.assertEqual(result['rows'], [])
        for token in tokens:
            self.api(path + '/scan/cancel', {'cursor': token})

    def test_arrow_small_batches_and_selective_rows_fit_real_ipc_pages(self):
        import hashlib
        import pyarrow as pa
        cid = str(uuid.uuid4())
        fields = [{'name': 'key', 'type': 'string', 'nullable': False}] + [
            {'name': 'n' + str(i), 'type': 'int64', 'nullable': False} for i in range(63)]
        self.api('/collections', {'collection_id': cid, 'name': 'ipc_wide', 'schema': {'fields': fields}})
        path = '/collections/' + cid
        # Cross the 128-batch coalescing boundary without exhausting the
        # cursor idle timeout under sanitizer overhead.
        for _ in range(4):
            rows = [dict({'key': 'same'}, **{'n' + str(i): str(int(index % 4 == 0)) for i in range(63)})
                    for index in range(256)]
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
        for body, expected in [({'batch_rows': 1, 'limit': 512}, 512),
            ({'batch_rows': 4, 'predicate': {'op': 'eq', 'field': 'n0', 'value': '1'}}, 256)]:
            count = 0
            while True:
                status, headers, data = self.request(path + '/scan', body, accept='application/vnd.apache.arrow.stream')
                self.assertEqual(status, 200, data[:1024])
                self.assertLessEqual(len(data), 16 * 1024 * 1024)
                self.assertEqual(hashlib.sha256(data).hexdigest(), headers['X-Content-SHA256'])
                batches = list(pa.ipc.open_stream(data))
                self.assertLessEqual(len(batches), 1, 'small core batches must be coalesced for IPC')
                count += sum(batch.num_rows for batch in batches)
                cursor = headers.get('X-GlamDB-Next-Cursor')
                if cursor is None:
                    break
                body = {'cursor': cursor}
            self.assertEqual(count, expected)

    def test_idle_cursor_expires_and_releases_capacity(self):
        import time
        path = self.collection()
        tokens = [self.api(path + '/scan/open', {})['cursor'] for _ in range(8)]
        time.sleep(62)
        self.api(path + '/scan', {'cursor': tokens[0]}, status=404)
        self.assertTrue(self.api(path + '/scan/open', {})['cursor'])


# unittest must not pick up imported or inherited unrelated tests.
for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(ScanHTTP, name, None)
del NativeHTTP

if __name__ == '__main__':
    unittest.main()
