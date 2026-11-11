"""Managed HTTP + real storage; deterministic SQLite VFS faults, not power-loss simulation."""
import http.client
import json
import select
import socket
import subprocess
import sys
import tempfile
import unittest
import uuid
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DRIVER = sys.argv.pop(1)
TOKEN = 'fault-test-token'


class NativeFaultHTTP(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix='glamdb-http-fault-', dir='/tmp')
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name) / 'server'
        self.start()
        self.addCleanup(self.stop)
        self.database = self.api('/databases', {'name': 'faults'})['database_id']
        self.base = '/databases/' + self.database

    def start(self):
        arguments = [DRIVER, str(self.directory)]
        if hasattr(self, 'connection_limit'):
            arguments.append(str(self.connection_limit))
        self.process = subprocess.Popen(arguments, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1)
        self.assertTrue(select.select([self.process.stdout], [], [], 20)[0], 'server startup timed out')
        line = self.process.stdout.readline()
        self.assertTrue(line, 'fault server did not report its address')
        self.port = json.loads(line)['port']

    def stop(self, *, terminate=True):
        if terminate and self.process.poll() is None:
            self.process.terminate()
        out, err = self.process.communicate(timeout=20)
        self.assertEqual(self.process.returncode, 0, out + err)

    def arm(self, target, commit, after):
        self.process.stdin.write(f'{target} {commit} {int(after)}\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 10)[0], 'fault control timed out')
        self.assertEqual(self.process.stdout.readline().strip(), 'armed')

    def wait_for_publication(self):
        self.process.stdin.write('wait\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 12)[0])
        self.assertEqual(self.process.stdout.readline().strip(), 'paused')

    def control(self, command):
        self.process.stdin.write(command + '\n')
        self.process.stdin.flush()
        self.assertTrue(select.select([self.process.stdout], [], [], 10)[0], 'control timed out')
        return self.process.stdout.readline().strip()

    def request(self, path, body=None):
        connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=15)
        connection.request('GET' if body is None else 'POST', path,
            None if body is None else json.dumps(body),
            {'Authorization': 'Bearer ' + TOKEN, 'Content-Type': 'application/json'})
        response = connection.getresponse()
        data = response.read()
        connection.close()
        return response.status, json.loads(data)

    def api(self, path, body=None, status=200):
        code, data = self.request(path, body)
        self.assertEqual(code, status, data)
        return data

    def collection(self):
        cid = str(uuid.uuid4())
        body = {'collection_id': cid, 'name': cid,
                'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}
        self.api(self.base + '/collections', body)
        return self.base + '/collections/' + cid

    def test_discard_only_removes_uninitialized_reservations(self):
        self.arm('core', 1, False)
        self.api('/databases', {'name': 'abandoned'}, status=503)
        self.arm('core', 0, False)
        abandoned = next(item for item in self.api('/databases')['items'] if item['name'] == 'abandoned')
        path = '/databases/' + abandoned['database_id']
        self.assertTrue(self.api(path + '/discard', {})['discarded'])
        self.api(path, status=404)
        self.api(self.base + '/discard', {}, status=412)
        self.assertEqual(len(self.api('/databases')['items']), 1)
        self.stop()
        self.start()
        self.api(path, status=404)
        replacement = self.api('/databases', {'name': 'abandoned'})
        self.assertNotEqual(replacement['database_id'], abandoned['database_id'])

    def test_connection_ceiling_waits_for_capacity_without_reset(self):
        self.stop()
        self.connection_limit = 2
        self.start()
        streams = []
        try:
            for _ in range(2):
                stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
                streams.append(stream)
                # Continue proves this socket was accepted and owns a body slot.
                stream.sendall((f'POST {self.base}/open HTTP/1.1\r\nHost: local\r\n'
                    f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                    'Content-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
                response = b''
                while not response.endswith(b'\r\n\r\n'):
                    chunk = stream.recv(1)
                    self.assertTrue(chunk, 'listener closed before Continue')
                    response += chunk
                self.assertEqual(response, b'HTTP/1.1 100 Continue\r\n\r\n')
            pending = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            streams.append(pending)
            pending.sendall(b'GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n')
            self.assertFalse(select.select([pending], [], [], 0.1)[0], 'listener must wait instead of closing overflow')
            streams[0].close()
            response = http.client.HTTPResponse(pending)
            response.begin()
            self.assertEqual(response.status, 200)
            self.assertEqual(json.loads(response.read())['status'], 'live')
            # A second complete body header fills the ceiling again. Shutdown
            # must cancel the paused accept timer and close both idle bodies.
            pending.close()
            stream = socket.create_connection(('127.0.0.1', self.port), timeout=5)
            streams.append(stream)
            stream.sendall((f'POST {self.base}/open HTTP/1.1\r\nHost: local\r\n'
                f'Authorization: Bearer {TOKEN}\r\nContent-Type: application/json\r\n'
                'Content-Length: 2\r\nExpect: 100-continue\r\n\r\n').encode())
            response = b''
            while not response.endswith(b'\r\n\r\n'):
                chunk = stream.recv(1)
                self.assertTrue(chunk, 'listener closed before Continue')
                response += chunk
            self.assertEqual(response, b'HTTP/1.1 100 Continue\r\n\r\n')
            self.stop()
        finally:
            for stream in streams:
                stream.close()

    def test_discards_waiting_on_close_do_not_starve_control_requests(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'held'}]}
        self.arm('pause', 1, False)
        with ThreadPoolExecutor(max_workers=4) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            close = workers.submit(self.api, self.base + '/close', {})
            discards = []
            try:
                deadline = time.monotonic() + 5
                while self.api(self.base)['state'] != 'closing':
                    self.assertLess(time.monotonic(), deadline)
                health = self.api(self.base + '/readyz', status=503)
                self.assertEqual(health['state'], 'stopping')
                self.assertFalse(health['read_available'])
                self.assertFalse(health['write_ready'])
                discards = [workers.submit(self.request, self.base + '/discard', {}) for _ in range(2)]
                # Exercise controls while both discard requests have time to
                # reach the server; they must never occupy the control workers.
                deadline = time.monotonic() + 0.3
                while time.monotonic() < deadline:
                    connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=0.5)
                    try:
                        connection.request('GET', '/databases', headers={'Authorization': 'Bearer ' + TOKEN})
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200, response.read())
                    except TimeoutError:
                        self.fail('discard requests starved database descriptors behind close')
                    finally:
                        connection.close()
            finally:
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
            self.assertEqual(close.result(timeout=10)['state'], 'closed')
            for discard in discards:
                self.assertIn(discard.result(timeout=10)[0], (412, 503))

    def test_listener_recovers_after_descriptor_exhaustion(self):
        self.arm('exhaust-fds', 0, False)
        try:
            with socket.create_connection(('127.0.0.1', self.port), timeout=5) as connection:
                connection.sendall(b'GET /healthz HTTP/1.1\r\nHost: local\r\n\r\n')
                # Let at least one accept report EMFILE before restoring capacity.
                time.sleep(0.3)
        finally:
            self.arm('release-fds', 0, False)
        self.assertEqual(self.api('/healthz')['status'], 'live')

    def test_blocked_database_does_not_starve_controls_or_other_database(self):
        path = self.collection()
        other = self.api('/databases', {'name': 'independent'})['database_id']
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'blocked'}]}
        self.arm('pause', 1, False)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            try:
                self.assertEqual(self.api('/healthz')['status'], 'live')
                self.assertEqual(len(self.api('/databases')['items']), 2)
                self.assertEqual(self.api('/databases/' + other + '/collections')['items'], [])
                self.assertEqual(self.api(self.base)['state'], 'open')
                self.assertEqual(self.api(self.base + '/open', {})['state'], 'open')
            finally:
                self.arm('release', 0, False)
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')

    def test_readiness_during_open_recovery_and_reconciliation(self):
        self.api(self.base + '/close', {})
        for action in ('open', 'reconcile'):
            with self.subTest(action=action):
                self.arm('pause', 1, False)
                with ThreadPoolExecutor(max_workers=1) as workers:
                    pending = workers.submit(self.api, self.base + '/' + action, {})
                    self.wait_for_publication()
                    try:
                        health = self.api(self.base + '/readyz', status=503)
                        self.assertEqual(health['state'], 'recovering')
                        self.assertFalse(health['read_available'])
                        self.assertFalse(health['write_ready'])
                        self.assertEqual(self.api(self.base + '/capabilities')['health'], health)
                        self.assertEqual(self.api('/healthz')['status'], 'live')
                    finally:
                        self.arm('release', 0, False)
                    pending.result(timeout=10)
                self.assertTrue(self.api(self.base + '/readyz')['write_ready'])

    def test_outcome_lookups_do_not_starve_controls_or_other_databases(self):
        path = self.collection()
        original = self.api(path + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'original'}]})
        other = self.api('/databases', {'name': 'quiet'})['database_id']
        collection = str(uuid.uuid4())
        quiet = '/databases/' + other + '/collections/' + collection
        self.api('/databases/' + other + '/collections', {'collection_id': collection,
            'name': 'quiet', 'schema': {'fields': [{'name': 'key', 'type': 'string'}]}})
        quiet_receipt = self.api(quiet + '/append',
            {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'quiet'}]})
        self.arm('pause', 2, False)  # Pause outer publication, holding the catalog mutex.
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append',
                {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'paused'}]})
            self.wait_for_publication()
            sockets = []
            try:
                operations = [original['operation_id']] + [str(uuid.uuid4()) for _ in range(7)]
                for operation in operations:
                    stream = socket.create_connection(('127.0.0.1', self.port), timeout=2)
                    sockets.append(stream)
                    stream.sendall((f'GET {path}/operations/{operation} HTTP/1.1\r\n'
                        f'Host: local\r\nAuthorization: Bearer {TOKEN}\r\n\r\n').encode())
                # Every lookup must refuse promptly while the publication
                # barrier remains held, including an older committed operation.
                for stream in sockets:
                    response = http.client.HTTPResponse(stream)
                    response.begin()
                    failure = json.loads(response.read())
                    self.assertEqual(response.status, 503, failure)
                    self.assertEqual(failure['error']['code'], 'OPERATION_IN_PROGRESS')
                    self.assertNotIn('outcome', failure)
                for target in ('/databases', self.base, '/databases/' + other,
                        quiet + '/operations/' + quiet_receipt['operation_id'],
                        self.base + '/readyz', self.base + '/capabilities'):
                    connection = http.client.HTTPConnection('127.0.0.1', self.port, timeout=1)
                    try:
                        connection.request('GET', target,
                            headers={'Authorization': 'Bearer ' + TOKEN})
                        response = connection.getresponse()
                        self.assertEqual(response.status, 200, response.read())
                    finally:
                        connection.close()
            finally:
                self.arm('release', 0, False)
                for stream in sockets:
                    stream.close()
            self.assertEqual(append.result(timeout=10)['outcome'], 'committed')
        self.assertEqual(self.api(path + '/operations/' + original['operation_id']), original)
        self.api(path + '/operations/' + operations[-1], status=404)

    def test_control_acknowledgements_after_startup(self):
        # The readiness record and control replies come from different threads.
        # TSan must see no shared stream-state race during this handshake.
        for _ in range(4):
            self.arm('core', 0, False)
            self.arm('registry', 0, False)
            self.arm('pause', 1, False)
            self.arm('release', 0, False)
        self.assertEqual(self.api(self.base + '/collections')['items'], [])

    def test_uncertain_create_commit_recovers_identity_without_duplicate_collection(self):
        for after in (False, True):
            with self.subTest(after_unlink=after):
                cid = str(uuid.uuid4())
                body = {'collection_id': cid, 'name': cid,
                        'schema': {'fields': [{'name': 'key', 'type': 'string'}]}}
                self.arm('core', 1, after)
                unknown = self.api(self.base + '/collections', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['recovery']['collection_id'], cid)
                self.assertEqual(unknown['recovery']['exact_name'], cid)
                self.assertTrue(unknown['recovery_required'])
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                created = self.api(self.base + '/collections', body)
                self.assertEqual(created['collection_id'], cid)
                self.assertEqual(self.api(self.base + '/collections', body), created)
        self.assertEqual(len(self.api(self.base + '/collections')['items']), 2)

    def test_uncertain_append_commit_and_recovery_fence_preserve_outcomes(self):
        path = self.collection()
        for after in (False, True):
            with self.subTest(after_unlink=after):
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(after)}]}
                self.arm('core', 2, after)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['phase'], 'catalog_commit')
                self.assertTrue(unknown['recovery']['publication_started'])
                lookup = path + '/operations/' + body['operation_id']
                lookup_unknown = self.api(lookup, status=503)
                self.assertEqual(lookup_unknown['outcome'], 'commit_unknown')
                self.assertEqual(lookup_unknown['recovery'], unknown['recovery'])
                self.assertEqual(self.api(path + '/append', body, status=503)['recovery'], unknown['recovery'])
                conflict = self.api(path + '/append', dict(body, rows=[{'key': 'different'}]), status=409)
                self.assertEqual(conflict['error']['code'], 'OPERATION_CONFLICT')
                blocked = self.api(path + '/append', {'operation_id': str(uuid.uuid4()),
                    'rows': [{'key': 'must-not-publish'}]}, status=503)
                self.assertEqual(blocked['error']['code'], 'RECOVERY_REQUIRED')
                self.arm('core', 1, True)
                self.api(self.base + '/reconcile', {}, status=503)
                self.api(lookup, status=503)
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                code, resolved = self.request(lookup)
                self.assertEqual(resolved['outcome'], 'committed' if after else 'definitely_not_committed')
                self.assertEqual(self.request(path + '/append', body), (code, resolved))
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'True'}])
        self.assertTrue(list(self.directory.rglob('*.parquet')), 'test must exercise native files')
        self.stop()
        self.start()
        self.assertEqual(self.request(lookup), (code, resolved))

    def test_binding_commit_fault_does_not_publish_or_invent_a_terminal_receipt(self):
        path = self.collection()
        for after in (False, True):
            with self.subTest(after_unlink=after):
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': str(after)}]}
                self.arm('core', 1, after)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['phase'], 'binding')
                health = self.api(self.base + '/readyz', status=503)
                self.assertEqual(health['state'], 'recovery_required')
                self.assertFalse(health['write_ready'])
                self.assertFalse(unknown['recovery']['publication_started'])
                self.arm('core', 0, False)
                self.api(self.base + '/reconcile', {})
                lookup = path + '/operations/' + body['operation_id']
                if after:
                    code, failure = self.request(lookup)
                    self.assertEqual(failure['outcome'], 'definitely_not_committed')
                    self.assertEqual(self.request(path + '/append', body), (code, failure))
                else:
                    self.api(lookup, status=404)
                    self.api(path + '/append', body)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'False'}])

    def test_lost_failure_recording_commit_retains_original_precondition_error(self):
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'existing'}]})
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'rejected'}], 'expected_snapshot': None}
        self.arm('core', 2, True)
        unknown = self.api(path + '/append', body, status=503)
        self.assertEqual(unknown['phase'], 'recording_failure')
        self.assertEqual(unknown['recovery']['first_error']['code'], 'PRECONDITION_FAILED')
        self.assertFalse(unknown['recovery']['publication_started'])
        self.arm('core', 0, False)
        self.api(self.base + '/reconcile', {})
        failure = self.api(path + '/append', body, status=412)
        self.assertEqual(failure['outcome'], 'definitely_not_committed')
        self.assertEqual(failure['error'], unknown['recovery']['first_error'])
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id'], status=412), failure)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'existing'}])

    def test_native_producer_path_failure_and_lost_failure_record_are_recoverable(self):
        path = self.collection()
        cid = path.rsplit('/', 1)[1]
        data_dir = self.directory / 'databases' / self.database / 'warehouse' / cid / 'data'
        saved = Path(self.tmp.name) / 'saved-data'
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'never-published'}]}
        self.arm('pause', 1, False)
        self.arm('core', 2, True)
        with ThreadPoolExecutor(max_workers=1) as workers:
            append = workers.submit(self.api, path + '/append', body, 503)
            self.wait_for_publication()
            # Preflight passed and binding is at COMMIT. A real ENOTDIR during
            # data-file production must survive a lost failure-record COMMIT.
            data_dir.rename(saved)
            data_dir.write_bytes(b'not a directory')
            try:
                self.arm('release', 0, False)
                unknown = append.result(timeout=10)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                self.assertEqual(unknown['phase'], 'recording_failure')
                self.assertEqual(unknown['recovery']['first_error']['code'], 'CORRUPTION')
                self.assertFalse(unknown['recovery']['publication_started'])
            finally:
                data_dir.unlink()
                saved.rename(data_dir)
                self.arm('core', 0, False)
        self.api(self.base + '/reconcile', {})
        failure = self.api(path + '/append', body, status=500)
        self.assertEqual(failure['outcome'], 'definitely_not_committed')
        self.assertEqual(failure['error'], unknown['recovery']['first_error'])
        self.assertEqual(self.api(path + '/scan', {})['rows'], [])
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'recovered'}]})
        self.stop()
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id'], status=500), failure)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'recovered'}])

    def test_registry_open_and_close_commit_faults_recover_persisted_intent(self):
        for action in ('close', 'open'):
            for after in (False, True):
                with self.subTest(action=action, after_unlink=after):
                    self.api(self.base + '/open', {})
                    if action == 'open':
                        self.api(self.base + '/close', {})
                    self.arm('registry', 1, after)
                    self.api(self.base + '/' + action, {}, status=503)
                    self.api('/databases', {'name': 'blocked'}, status=503)
                    self.arm('registry', 0, False)
                    self.stop()
                    self.start()
                    expected = ('closed' if after else 'open') if action == 'close' else ('open' if after else 'closed')
                    self.assertEqual(self.api(self.base)['state'], expected)
                    self.assertEqual(len(self.api('/databases')['items']), 1)

    def test_registry_create_commit_faults_preserve_reservation_and_complete_store(self):
        for commit in (1, 2):
            for after in (False, True):
                with self.subTest(commit=commit, after_unlink=after):
                    database = str(uuid.uuid4())
                    body = {'name': database, 'database_id': database}
                    root = self.directory / 'databases' / database
                    self.arm('registry', commit, after)
                    self.api('/databases', body, status=503)
                    self.assertEqual(root.exists(), commit == 2)
                    identity = root / 'store.identity'
                    inode = identity.stat().st_ino if identity.exists() else None
                    self.api('/databases', body, status=503)
                    self.arm('registry', 0, False)
                    self.stop()
                    self.start()
                    created = self.api('/databases', body)
                    self.assertEqual(created['state'], 'open')
                    self.assertEqual(created['database_id'], database)
                    self.assertEqual(self.api('/databases', body), created)
                    if inode is not None:
                        self.assertEqual(identity.stat().st_ino, inode, 'complete store was replaced')

    def test_startup_store_failure_isolated_and_identity_never_replaced(self):
        path = self.collection()
        self.api(path + '/append', {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'kept'}]})
        other = self.api('/databases', {'name': 'unaffected'})['database_id']
        root = self.directory / 'databases' / self.database
        other_root = self.directory / 'databases' / other
        self.stop()
        saved = self.directory / 'saved'
        root.rename(saved)
        other_root.rename(root)
        saved.rename(other_root)
        self.start()
        self.assertEqual(self.api(self.base)['state'], 'error')
        self.assertEqual(self.api('/databases/' + other)['state'], 'error')
        new = self.api('/databases', {'name': 'healthy'})
        self.assertEqual(new['state'], 'open')
        error = self.api(self.base + '/open', {}, status=409)
        self.assertEqual(error['error']['code'], 'STORE_IDENTITY_MISMATCH')
        self.assertTrue(list(root.rglob('store.identity')))
        self.stop()
        root.rename(saved)
        other_root.rename(root)
        saved.rename(other_root)
        self.start()
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'kept'}])

    def test_shutdown_timeout_keeps_native_publication_and_ownership(self):
        path = self.collection()
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'admitted'}, {'key': 'admitted'}]}
        self.assertEqual(self.control('owner ' + self.database), 'STATE_IN_USE')
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=1) as pool:
            append = pool.submit(self.api, path + '/append', body)
            try:
                self.wait_for_publication()
                self.assertEqual(self.control('shutdown ' + self.database), 'DEADLINE_EXCEEDED')
                self.assertEqual(self.control('owner ' + self.database), 'STATE_IN_USE')
                self.assertFalse(self.api(self.base + '/readyz', status=503)['write_ready'])
                self.process.terminate()
                try:
                    self.process.wait(timeout=0.2)
                except subprocess.TimeoutExpired:
                    pass
                else:
                    self.fail('server exited during native publication')
                self.assertEqual(self.control('server-state'), 'running')
            finally:
                self.arm('release', 0, False)
            outcome = append.result(timeout=10)
            self.assertEqual(outcome['outcome'], 'committed')
            self.assertEqual(outcome['result']['rows_appended'], '2')
        self.stop(terminate=False)
        self.start()
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), outcome)
        self.assertEqual(self.api(path + '/append', body), outcome)
        self.assertEqual(self.api(path + '/scan', {})['rows'], body['rows'])

    def test_shutdown_reconciles_uncertain_native_publication_on_restart(self):
        for graceful in (True, False):
            with self.subTest(graceful=graceful):
                path = self.collection()
                body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'uncertain'}]}
                self.arm('core', 2, True)
                unknown = self.api(path + '/append', body, status=503)
                self.assertEqual(unknown['outcome'], 'commit_unknown')
                if graceful:
                    self.stop()
                else:
                    self.process.kill()
                    self.process.communicate(timeout=20)
                    self.assertEqual(self.process.returncode, -9)
                self.start()
                outcome = self.api(path + '/operations/' + body['operation_id'])
                self.assertEqual(outcome['outcome'], 'committed')
                self.assertEqual(self.api(path + '/append', body), outcome)
                self.assertEqual(self.api(path + '/scan', {})['rows'], body['rows'])

    def test_close_drains_native_publication_and_other_database_stays_available(self):
        path = self.collection()
        other = self.api('/databases', {'name': 'other'})['database_id']
        body = {'operation_id': str(uuid.uuid4()), 'rows': [{'key': 'admitted'}]}
        self.arm('pause', 2, False)
        with ThreadPoolExecutor(max_workers=2) as workers:
            append = workers.submit(self.api, path + '/append', body)
            self.wait_for_publication()
            close = workers.submit(self.api, self.base + '/close', {})
            try:
                deadline = time.monotonic() + 5
                while time.monotonic() < deadline:
                    status, _ = self.request(self.base + '/collections')
                    if status == 429:
                        break
                    time.sleep(0.01)
                else:
                    self.fail('close did not reject new database work')
                self.assertFalse(close.done(), 'close released the store before publication finished')
                self.assertEqual(self.api('/databases/' + other + '/collections')['items'], [])
            finally:
                self.arm('release', 0, False)
            outcome = append.result(timeout=10)
            self.assertEqual(outcome['outcome'], 'committed')
            self.assertEqual(close.result(timeout=10)['state'], 'closed')
        self.api(self.base + '/open', {})
        self.assertEqual(self.api(path + '/operations/' + body['operation_id']), outcome)
        self.assertEqual(self.api(path + '/scan', {})['rows'], [{'key': 'admitted'}])


if __name__ == '__main__':
    unittest.main()
