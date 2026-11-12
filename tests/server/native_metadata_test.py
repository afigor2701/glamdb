"""Public metadata over actual native HTTP and the SDK; no mock inventory."""
import json
import os
from pathlib import Path
import select
import subprocess
import sys
import tempfile
import unittest
import uuid
from urllib.parse import quote

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'sdk/python/src'))
_test_arguments = sys.argv[4:]
from native_http_test import CLI, NativeHTTP, SERVER, TOKEN
sys.argv[1:] = _test_arguments


class NativeMetadataHTTP(NativeHTTP):
    def test_literal_metadata_vectors_match_the_wire_schema(self):
        from jsonschema import Draft202012Validator
        root = Path(__file__).resolve().parents[2]
        fixture = json.loads((root / 'fixtures/http/metadata-v0.json').read_text())
        wire = json.loads((root / 'fixtures/http/v0.schema.json').read_text())
        for case in fixture['cases']:
            validator = Draft202012Validator(wire | {'$ref': '#/$defs/' + case['response_schema']})
            validator.validate(case['response'])
            for missing in ('identity', 'snapshot_id'):
                incomplete = dict(case['response'])
                del incomplete[missing]
                self.assertFalse(validator.is_valid(incomplete), (case['method'], missing))

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(dir=str(Path(os.environ.get('GLAMDB_TEST_TMPDIR', '/tmp')).resolve()))
        self.addCleanup(self.tmp.cleanup)
        self.data_dir = str(Path(self.tmp.name) / 'server')
        self.store_id = str(uuid.uuid4())
        self.env = dict(os.environ, GLAMDB_TOKEN=TOKEN)
        self.assertTrue(Path(SERVER).is_file(), 'native server executable is missing')
        self.root = str(Path(self.data_dir) / 'databases' / self.store_id)
        self.start()
        self.addCleanup(self.stop)
        self.api('/databases', {'database_id': self.store_id, 'name': 'test_database'})

    def seed(self):
        path = self.collection()
        a = self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [
            {'key': 'duplicate', 'n': '9007199254740993', 'ts': '2026-01-01T00:00:00.123456Z'},
            {'key': 'duplicate', 'n': None, 'ts': None}]})['result']
        b = self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [
            {'key': 'later', 'n': '-9223372036854775808', 'ts': '1969-12-31T23:59:59.999999Z'}]})['result']
        return path, a, b

    def footer_codecs(self, operation_id):
        """Codecs recorded by the footer of one append's file, read independently of the engine."""
        import pyarrow.parquet as pq
        files = [path for path in Path(self.root).rglob('*.parquet') if operation_id in path.name]
        self.assertEqual(len(files), 1, files)
        footer = pq.ParquetFile(files[0]).metadata
        codecs = {}
        for group in range(footer.num_row_groups):
            for column in range(footer.num_columns):
                chunk = footer.row_group(group).column(column)
                codecs.setdefault(chunk.path_in_schema, set()).add(chunk.compression)
        return {name: values.pop() if len(values) == 1 else values for name, values in codecs.items()}

    def test_parquet_write_settings_reach_footers_restart_and_retries(self):
        from jsonschema import Draft202012Validator
        wire = json.loads((Path(__file__).resolve().parents[2] / 'fixtures/http/v0.schema.json').read_text())
        collection_schema = Draft202012Validator(wire | {'$ref': '#/$defs/collection'})
        capabilities = self.api('/databases/' + self.store_id + '/capabilities')
        self.assertTrue(capabilities['features']['write_settings'])
        codecs = {item['codec']: item['level'] for item in capabilities['parquet']['codecs']}
        self.assertEqual(codecs['zstd'], {'min': 1, 'max': 22, 'default': 1})
        self.assertIsNone(codecs['snappy'])
        self.assertEqual(capabilities['parquet']['default_compression'], {'codec': 'zstd', 'level': None})

        cid = str(uuid.uuid4())
        path = '/collections/' + cid
        settings = {'parquet': {'compression': {'codec': 'gzip', 'level': 6},
                                'columns': [{'field': 'ts', 'compression': {'codec': 'lz4_raw'}},
                                            {'field': 'key', 'compression': {'codec': 'uncompressed'}}]}}
        body = {'collection_id': cid, 'name': 'settings', 'write_settings': settings, 'schema': {'fields': [
            {'name': 'key', 'type': 'string', 'nullable': False},
            {'name': 'n', 'type': 'int64', 'nullable': True},
            {'name': 'ts', 'type': 'timestamp_us_utc', 'nullable': True}]}}
        canonical = {'parquet': {'compression': {'codec': 'gzip', 'level': 6},
                                 'columns': [{'field': 'key', 'compression': {'codec': 'uncompressed', 'level': None}},
                                             {'field': 'ts', 'compression': {'codec': 'lz4_raw', 'level': None}}]}}
        created = self.api('/collections', body)
        self.assertEqual(created['write_settings'], canonical)
        info = self.api(path)
        collection_schema.validate(info)
        self.assertEqual(info['write_settings'], canonical)
        self.assertEqual(info['write_settings_version'], '1')
        # Creation is idempotent only for the same settings.
        self.assertEqual(self.api('/collections', body), info)
        mismatch = self.api('/collections', dict(body, write_settings={'parquet': None}), status=409)
        self.assertEqual(mismatch['error']['code'], 'WRITE_SETTINGS_MISMATCH')

        rows = [{'key': 'dup', 'n': '9007199254740993', 'ts': '2026-01-01T00:00:00.000001Z'},
                {'key': 'dup', 'n': None, 'ts': None}]
        first = str(uuid.uuid4())
        receipt = self.api(path + '/append', {'operation_id': first, 'rows': rows})
        # PyArrow names the Parquet LZ4_RAW codec "LZ4" (Hadoop-framed LZ4 is "LZ4_HADOOP").
        self.assertEqual(self.footer_codecs(first), {'key': 'UNCOMPRESSED', 'n': 'GZIP', 'ts': 'LZ4'})
        files = self.api(path + '/files')['items']
        details = self.api(path + '/files/' + files[0]['file_id'])
        reported = {column['field_id']: column['compression']
                    for group in details['row_groups'] for column in group['field_metrics']}
        self.assertEqual(reported, {1: 'uncompressed', 2: 'gzip', 3: 'lz4_raw'})

        update = {'expected_version': '1', 'write_settings': {'parquet': {'compression': {'codec': 'snappy'}}}}
        updated = self.api(path + '/write-settings', update)
        collection_schema.validate(updated)
        self.assertEqual(updated['write_settings_version'], '2')
        # A lost response is retried with the same request.
        self.assertEqual(self.api(path + '/write-settings', update), updated)
        boundary = self.api(path + '/write-settings', dict(update, expected_version='9223372036854775807'), status=400)
        self.assertEqual(boundary['error']['code'], 'INVALID_ARGUMENT')
        conflict = self.api(path + '/write-settings', dict(update, write_settings={'parquet': None}), status=409)
        self.assertEqual(conflict['error']['code'], 'WRITE_SETTINGS_CONFLICT')
        for invalid in ({'parquet': {'compression': {'codec': 'brotli'}}},
                        {'parquet': {'compression': {'codec': 'zstd', 'level': 23}}},
                        {'parquet': {'compression': {'codec': 'snappy', 'level': 1}}},
                        {'parquet': {'columns': [{'field': 'missing', 'compression': {'codec': 'zstd'}}]}},
                        {'parquet': {'columns': [{'field': 'key'}]}},
                        {'parquet': {'compression': {'codec': 'zstd'}, 'extra': True}},
                        {'transport': {}}):
            with self.subTest(invalid=invalid):
                error = self.api(path + '/write-settings', {'expected_version': '2', 'write_settings': invalid},
                                 status=400)
                self.assertEqual(error['error']['code'], 'INVALID_ARGUMENT')
        second = str(uuid.uuid4())
        self.api(path + '/append', {'operation_id': second, 'rows': rows[:1]})
        self.assertEqual(self.footer_codecs(second), {'key': 'SNAPPY', 'n': 'SNAPPY', 'ts': 'SNAPPY'})
        # Old files keep their codecs; the retried first append returns its receipt.
        self.assertEqual(self.footer_codecs(first), {'key': 'UNCOMPRESSED', 'n': 'GZIP', 'ts': 'LZ4'})
        self.assertEqual(self.api(path + '/append', {'operation_id': first, 'rows': rows}), receipt)

        self.stop()
        self.start()
        self.assertEqual(self.api(path)['write_settings'],
                         {'parquet': {'compression': {'codec': 'snappy', 'level': None}, 'columns': []}})
        scanned = self.api(path + '/scan', {})['rows']
        self.assertEqual(sorted(json.dumps(row, sort_keys=True) for row in scanned),
                         sorted(json.dumps(row, sort_keys=True) for row in rows + rows[:1]))
        historical = self.api(path + '/scan', {'snapshot_id': receipt['result']['snapshot_id']})['rows']
        self.assertEqual(historical, rows)
        cli = subprocess.run([CLI, 'request', '--port', str(self.port), '--method', 'POST',
                              '--path', '/databases/' + self.store_id + path + '/write-settings', '--body', '-'],
                             input=json.dumps({'expected_version': '2', 'write_settings': {'parquet': None}}),
                             env=self.env, check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(cli.stdout)['write_settings_version'], '3')

    def test_zero_column_explain_matches_the_wire_schema(self):
        from jsonschema import Draft202012Validator
        root = Path(__file__).resolve().parents[2]
        wire = json.loads((root / 'fixtures/http/v0.schema.json').read_text())
        validator = Draft202012Validator(wire | {'$ref': '#/$defs/explanation'})
        path, a, b = self.seed()
        for selector, snapshot in [({'snapshot_id': 'initial'}, None),
                                   ({'snapshot_id': a['snapshot_id']}, a['snapshot_id']),
                                   ({}, b['snapshot_id'])]:
            with self.subTest(selector=selector):
                explained = self.api(path + '/explain', {
                    'projection': [], **selector,
                    'predicate': {'op': 'is_null', 'field': 'n'},
                })
                self.assertEqual(explained['snapshot_id'], snapshot)
                self.assertEqual(explained['schema']['fields'], [])
                self.assertEqual(explained['output_columns'], [])
                self.assertEqual(explained['read_columns'], ['n'])
                self.assertTrue(explained['capabilities']['features']['zero_column_projection'])
                validator.validate(explained)

    def test_selected_statistics_history_explain_and_precise_footer(self):
        path, a, b = self.seed()
        unscoped = self.api('/capabilities')
        self.assertTrue(unscoped['file_pruning'])
        self.assertTrue(unscoped['row_group_pruning'])
        scoped = self.api('/databases/' + self.store_id + '/capabilities')
        self.assertEqual(scoped['store_id'], self.store_id)
        for feature in ('stats', 'snapshots', 'files', 'explain', 'metadata_pagination'):
            self.assertTrue(scoped['features'][feature], feature)
        self.assertTrue(scoped['file_pruning'])
        self.assertTrue(scoped['row_group_pruning'])
        for selector, count, files in [('initial', '0', '0'), (a['snapshot_id'], '2', '1'), ('latest', '3', '2')]:
            stats = self.api(path + '/stats?snapshot_id=' + selector)
            self.assertEqual(stats['rows'], count)
            self.assertEqual(stats['data_files'], files)
            self.assertEqual(stats['retained_snapshots'], '2')
            self.assertEqual(stats['identity']['current_snapshot_id'], b['snapshot_id'])
            self.assertEqual(stats['snapshot_id'], None if selector == 'initial' else b['snapshot_id'] if selector == 'latest' else selector)
            self.assertIsNone(stats['logical_bytes'])
            self.assertIsNone(stats['root_metadata_bytes'])
        history = self.api(path + '/snapshots')['items']
        self.assertEqual([s['snapshot_id'] for s in history], [a['snapshot_id'], b['snapshot_id']])
        self.assertEqual([s['rows_added'] for s in history], ['2', '1'])
        self.assertEqual(history[1]['parent_snapshot_id'], a['snapshot_id'])
        self.assertEqual(history[0]['receipt'], a)
        self.assertEqual(self.api(path + '/snapshots?snapshot_id=' + a['snapshot_id'])['items'], history[:1])
        self.assertEqual(self.api(path + '/snapshots?snapshot_id=initial')['items'], [])
        initial = self.api(path + '/files?snapshot_id=initial')
        self.assertEqual(initial['items'], [])
        files_a = self.api(path + '/files?snapshot_id=' + a['snapshot_id'])
        files_b = self.api(path + '/files')
        self.assertEqual(len(files_a['items']), 1)
        self.assertEqual(len(files_b['items']), 2)
        self.assertEqual(sum(int(f['record_count']) for f in files_b['items']), 3)
        file = files_a['items'][0]
        self.assertIsNone(file['path'])
        self.assertRegex(file['file_id'], r'^[0-9a-f]{64}$')
        self.assertTrue(file['field_metrics'])
        self.assertFalse(file['synthetic'])
        detail = self.api(path + '/files/' + file['file_id'] + '?snapshot_id=' + a['snapshot_id'])
        self.assertEqual(detail['snapshot_id'], a['snapshot_id'])
        self.assertEqual(sum(int(g['record_count']) for g in detail['row_groups']), 2)
        by_name = {f['name']: f for f in detail['footer_statistics']}
        self.assertEqual({f['field_id'] for f in detail['footer_statistics']}, {1, 2, 3})
        self.assertEqual(by_name['n']['null_count'], '1')
        self.assertEqual(by_name['n']['lower_bound'], '9007199254740993')
        self.assertEqual(by_name['n']['upper_bound'], '9007199254740993')
        self.assertEqual(by_name['ts']['lower_bound'], '2026-01-01T00:00:00.123456Z')
        self.assertEqual(by_name['n']['bound_encoding'], 'parquet-plain')
        self.assertEqual(by_name['n']['lower_bound_hex'], '0100000000002000')
        self.assertIsNone(by_name['n']['distinct_count'])
        self.assertIsNone(by_name['n']['lower_bound_exact'])
        self.assertIsNone(detail['indexes']['bloom'])
        self.assertTrue(detail['execution_capabilities']['file_pruning'])
        self.assertTrue(detail['execution_capabilities']['row_group_pruning'])
        self.assertFalse(detail['execution_capabilities']['page_pruning'])
        self.assertFalse(detail['execution_capabilities']['bloom_pruning'])
        explained = self.api(path + '/explain', {'projection': ['key'], 'predicate': {'op': 'gt', 'field': 'n', 'value': '0'}, 'snapshot_id': a['snapshot_id']})
        self.assertEqual(explained['snapshot_id'], a['snapshot_id'])
        self.assertEqual(explained['output_columns'], ['key'])
        self.assertEqual(explained['read_columns'], ['key', 'n'])
        self.assertEqual(explained['estimates']['rows_before_filter'], '2')
        self.assertIsNone(explained['estimates']['output_rows'])
        self.assertIsNone(explained['actual_io'])
        self.assertIn('prune candidate files and Parquet row groups', explained['plan'])
        self.assertTrue(explained['capabilities']['file_pruning'])
        self.assertTrue(explained['capabilities']['row_group_pruning'])
        self.api('/databases/' + self.store_id + '/close', {})
        closed_caps = self.api('/databases/' + self.store_id + '/capabilities')
        self.assertTrue(closed_caps['file_pruning'])
        self.assertTrue(closed_caps['row_group_pruning'])
        self.api('/databases/' + self.store_id + '/open', {})
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/snapshots')['items'], history)
        self.assertEqual(self.api(path + '/files')['items'], files_b['items'])

    def test_explain_preserves_compound_predicates_and_null_literals(self):
        path = self.collection()
        predicates = [
            {'op': 'true'},
            {'op': 'eq', 'field': 'n', 'value': None},
            {'op': 'in', 'field': 'n', 'values': ['9007199254740993', None, '-9223372036854775808']},
            {'op': 'is_null', 'field': 'n'},
            {'op': 'is_not_null', 'field': 'ts'},
            {'op': 'and', 'args': [
                {'op': 'or', 'args': [
                    {'op': 'is_null', 'field': 'n'},
                    {'op': 'eq', 'field': 'key', 'value': 'точное значение'}]},
                {'op': 'not', 'arg': {'op': 'ge', 'field': 'ts', 'value': '2026-01-01T00:00:00.123456Z'}},
                {'op': 'in', 'field': 'n', 'values': ['9007199254740993', None]}]},
        ]
        for predicate in predicates:
            with self.subTest(predicate=predicate):
                explained = self.api(path + '/explain', {'projection': ['key'], 'predicate': predicate})
                self.assertEqual(explained['predicate'], predicate)

    def test_metadata_cursor_is_bound_to_view_parameters_and_database(self):
        path, a, b = self.seed()
        for kind in ('snapshots', 'files'):
            first = self.api(path + '/' + kind + '?page_size=1')
            cursor = quote(first['next_cursor'], safe='')
            target = path + '/' + kind + '?page_size=1&cursor=' + cursor
            # Later appends cannot change a retained page's identity or inventory.
            self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': kind}]})
            second = self.api(target)
            self.assertEqual(first['identity'], second['identity'])
            self.assertEqual(first['snapshot_id'], second['snapshot_id'])
            self.assertEqual(second, self.api(target))
            self.api(target.replace('page_size=1', 'page_size=2'), status=400)
            self.api(target + '&snapshot_id=' + a['snapshot_id'], status=400)
            other_kind = 'files' if kind == 'snapshots' else 'snapshots'
            self.api(target.replace('/' + kind + '?', '/' + other_kind + '?'), status=400)
            self.api(target + 'f', status=404)
            other = self.api('/databases', {'name': 'other_' + kind})['database_id']
            self.api('/databases/' + other + target, status=404)
            while second['next_cursor'] is not None:
                second = self.api(path + '/' + kind + '?page_size=1&cursor=' + quote(second['next_cursor'], safe=''))
        # Completed replay inventories make room for eight unread listings.
        for _ in range(8):
            opened = self.api(path + '/files?page_size=1')
        target = path + '/files?page_size=1&cursor=' + quote(opened['next_cursor'], safe='')
        self.api(path + '/files?page_size=1', status=429)
        old = self.api('/databases/' + self.store_id + '/close', {})
        self.assertEqual(old['state'], 'closed')
        self.api('/databases/' + self.store_id + '/open', {})
        self.api(target, status=404)
        self.api(path + '/files?page_size=1')

    def test_public_manifest_and_parquet_bound_encodings_keep_all_scalar_types(self):
        cid = str(uuid.uuid4())
        fields = [{'name': name, 'type': kind, 'nullable': name != 'key'} for name, kind in (
            ('key', 'string'), ('n', 'int64'), ('ts', 'timestamp_us_utc'),
            ('f', 'float64'), ('b', 'bool'), ('note', 'string'))]
        self.api('/collections', {'collection_id': cid, 'name': 'binary_bounds', 'schema': {'fields': fields}})
        path = '/collections/' + cid
        rows = [
            {'key': 'a', 'n': '-9223372036854775808', 'ts': '1969-12-31T23:59:59.999999Z',
             'f': -2.5, 'b': False, 'note': 'common-prefix-longer-than-sixteen-é'},
            {'key': 'b', 'n': '9223372036854775807', 'ts': '2026-01-01T00:00:00.123456Z',
             'f': 0.5, 'b': True, 'note': 'common-prefix-longer-than-sixteen-Я'},
            {'key': 'c'}]
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': rows})
        file = self.api(path + '/files')['items'][0]
        manifest = {field['name']: field for field in file['field_metrics']}
        details = self.api(path + '/files/' + file['file_id'])
        footer = {field['name']: field for field in details['row_groups'][0]['field_metrics']}
        for source, encoding in ((manifest, 'iceberg-single-value'), (footer, 'parquet-plain')):
            self.assertEqual(source['n']['lower_bound'], '-9223372036854775808')
            self.assertEqual(source['n']['upper_bound'], '9223372036854775807')
            self.assertEqual(source['n']['lower_bound_hex'], '0000000000000080')
            self.assertEqual(source['n']['upper_bound_hex'], 'ffffffffffffff7f')
            self.assertEqual(source['ts']['lower_bound'], '1969-12-31T23:59:59.999999Z')
            self.assertEqual(source['f']['lower_bound'], -2.5)
            self.assertEqual(source['f']['upper_bound'], 0.5)
            self.assertIs(source['b']['lower_bound'], False)
            self.assertIs(source['b']['upper_bound'], True)
            for field in source.values():
                self.assertEqual(field['bound_encoding'], encoding)
                self.assertIsNone(field['lower_bound_exact'])
                self.assertIsNone(field['upper_bound_exact'])
                self.assertIsNone(field['distinct_count'])
            self.assertEqual(source['note']['null_count'], '1')
        self.assertEqual(footer['note']['lower_bound'], rows[0]['note'])
        self.assertEqual(footer['note']['upper_bound'], rows[1]['note'])
        self.assertIsNone(details['indexes']['bloom'])
        self.assertGreater(int(details['footer_bytes']), 0)

    def test_invalid_snapshot_file_membership_traversal_and_parameters(self):
        path, a, b = self.seed()
        old = {f['file_id'] for f in self.api(path + '/files?snapshot_id=' + a['snapshot_id'])['items']}
        new_file = next(f['file_id'] for f in self.api(path + '/files')['items'] if f['file_id'] not in old)
        self.api(path + '/files/' + new_file + '?snapshot_id=' + a['snapshot_id'], status=404)
        self.api(path + '/files/' + new_file + '?snapshot_id=initial', status=404)
        for suffix in ('stats', 'snapshots', 'files', 'files/' + new_file):
            self.api(path + '/' + suffix + '?snapshot_id=9223372036854775807', status=404)
            for query in ('snapshot_id=00', 'snapshot_id=9223372036854775808', 'unexpected=1', 'snapshot_id=initial&snapshot_id=latest'):
                self.api(path + '/' + suffix + '?' + query, status=400)
        for value in ('../../catalog.sqlite', 'store://arbitrary/data.parquet', '0' * 64):
            status, _, _ = self.request(path + '/files/' + quote(value, safe=''))
            self.assertIn(status, (400, 404))
        self.api(path + '/files/' + '0' * 64, status=404)
        for query in ('page_size=0', 'page_size=1001', 'page_size=01', 'cursor=forged'):
            self.api(path + '/files?' + query, status=404 if query.startswith('cursor') else 400)

    def test_completed_metadata_refreshes_do_not_exhaust_database_sessions(self):
        path, _, _ = self.seed()
        expected = {}
        for refresh in range(12):
            for kind in ('snapshots', 'files'):
                target = path + '/' + kind + '?page_size=1'
                items = []
                while True:
                    page = self.api(target)
                    items.extend(page['items'])
                    if page['next_cursor'] is None:
                        break
                    target = path + '/' + kind + '?page_size=1&cursor=' + quote(page['next_cursor'], safe='')
                self.assertEqual(len(items), 2, (refresh, kind))
                if refresh == 0:
                    expected[kind] = items
                self.assertEqual(items, expected[kind])

    def test_retained_metadata_pages_obey_recovery_admission(self):
        path, _, _ = self.seed()
        opened = self.api(path + '/files?page_size=1')
        target = path + '/files?page_size=1&cursor=' + quote(opened['next_cursor'], safe='')
        prefix = 'store://' + self.store_id + '/'
        location = opened['identity']['metadata_location']
        self.assertTrue(location.startswith(prefix))
        metadata = Path(self.root) / location[len(prefix):]
        held = Path(self.tmp.name) / 'held-metadata.json'
        metadata.rename(held)
        try:
            self.assertEqual(self.api(path + '/stats', status=500)['error']['code'], 'CORRUPTION')
            self.assertEqual(self.api(target, status=503)['error']['code'], 'RECOVERY_REQUIRED')
        finally:
            held.rename(metadata)
        self.api('/databases/' + self.store_id + '/reconcile', {})
        self.api(target, status=404)

    def test_sdk_preserves_selected_metadata_pages_and_int64_bounds(self):
        from glamdb_sdk import Client
        path, a, b = self.seed()
        cid = path.rsplit('/', 1)[-1]
        with Client('http://127.0.0.1:' + str(self.port), TOKEN, database_id=self.store_id) as client:
            self.assertEqual(client.stats(cid, snapshot_id=int(a['snapshot_id']))['rows'], '2')
            self.assertEqual(client.stats(cid, snapshot_id=None)['rows'], '0')
            page = client.snapshots(cid, page_size=1)
            self.assertEqual(page.items[0]['snapshot_id'], a['snapshot_id'])
            self.assertEqual(client.snapshots(cid, page_size=1, cursor=page.next_cursor).items[0]['snapshot_id'], b['snapshot_id'])
            files = client.files(cid, snapshot_id=int(a['snapshot_id']))
            detail = client.file(cid, files.items[0]['file_id'], snapshot_id=int(a['snapshot_id']))
            self.assertEqual(next(f for f in detail['footer_statistics'] if f['name'] == 'n')['lower_bound'], '9007199254740993')
            self.assertEqual(client.explain(cid, snapshot_id=None)['snapshot_id'], None)

    def test_legacy_metadata_uses_the_same_public_facade(self):
        from glamdb_sdk import Client
        path, a, b = self.seed()
        self.stop()
        self.proc = subprocess.Popen([SERVER, '--root', self.root, '--store-id', self.store_id, '--port', '0'],
                                     env=self.env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        readable, _, _ = select.select([self.proc.stdout], [], [], 15)
        self.assertTrue(readable, 'legacy server did not report listening address')
        line = self.proc.stdout.readline()
        self.assertTrue(line, 'legacy server failed to open the test store')
        self.port = json.loads(line)['port']
        cid = path.rsplit('/', 1)[-1]
        with Client('http://127.0.0.1:' + str(self.port), TOKEN) as client:
            capabilities = client.capabilities()
            self.assertEqual(capabilities['store_id'], self.store_id)
            self.assertTrue(capabilities['features']['stats'])
            self.assertFalse(capabilities['features']['database_management'])
            self.assertEqual(client.stats(cid)['rows'], '3')
            self.assertEqual(client.stats(cid, snapshot_id=int(a['snapshot_id']))['rows'], '2')
            self.assertEqual([s['snapshot_id'] for s in client.snapshots(cid).items], [a['snapshot_id'], b['snapshot_id']])
            self.assertEqual(client.files(cid, snapshot_id=None).items, ())
            self.assertEqual(client.explain(cid, snapshot_id=int(a['snapshot_id']))['snapshot_id'], a['snapshot_id'])


for name in list(vars(NativeHTTP)):
    if name.startswith('test_'):
        setattr(NativeMetadataHTTP, name, None)
del NativeHTTP

if __name__ == '__main__':
    unittest.main()
