class FakeBucket(object):
    def __init__(self, client, name=None):
        self.client = client  # type: FakeClient
        self.name = name
        self.blobs = OrderedDict()
        self._exists = True

        #
        # This is simpler than creating a backend and metaclass to store the state of every bucket created
        #
        self.client.register_bucket(self)

    def blob(self, blob_id):
        return self.blobs.get(blob_id, FakeBlob(blob_id, self))

    def delete(self):
        self.client.delete_bucket(self)
        self._exists = False
        for blob in list(self.blobs.values()):
            blob.delete()

    def exists(self):
        return self._exists

    def get_blob(self, blob_id):
        try:
            return self.blobs[blob_id]
        except KeyError as e:
            raise google.cloud.exceptions.NotFound('Blob {} not found'.format(blob_id)) from e

    def list_blobs(self):
        return list(self.blobs.values())

    def delete_blob(self, blob):
        del self.blobs[blob.name]

    def register_blob(self, blob):
        if blob.name not in self.blobs.keys():
            self.blobs[blob.name] = blob

    def register_upload(self, upload):
        self.client.register_upload(upload)


class FakeBucketTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.bucket = FakeBucket(self.client, 'test-bucket')

    def test_blob_registers_with_bucket(self):
        blob_id = 'blob.txt'
        expected = FakeBlob(blob_id, self.bucket)
        actual = self.bucket.blob(blob_id)
        self.assertEqual(actual, expected)

    def test_blob_alternate_constuctor(self):
        blob_id = 'blob.txt'
        expected = self.bucket.blob(blob_id)
        actual = self.bucket.list_blobs()[0]
        self.assertEqual(actual, expected)

    def test_delete(self):
        blob_id = 'blob.txt'
        blob = FakeBlob(blob_id, self.bucket)
        self.bucket.delete()
        self.assertFalse(self.bucket.exists())
        self.assertFalse(blob.exists())

    def test_get_multiple_blobs(self):
        blob_one_id = 'blob_one.avro'
        blob_two_id = 'blob_two.parquet'
        blob_one = self.bucket.blob(blob_one_id)
        blob_two = self.bucket.blob(blob_two_id)
        actual_first_blob = self.bucket.get_blob(blob_one_id)
        actual_second_blob = self.bucket.get_blob(blob_two_id)
        self.assertEqual(actual_first_blob, blob_one)
        self.assertEqual(actual_second_blob, blob_two)

    def test_get_nonexistent_blob(self):
        with self.assertRaises(google.cloud.exceptions.NotFound):
            self.bucket.get_blob('test-blob')

    def test_list_blobs(self):
        blob_one = self.bucket.blob('blob_one.avro')
        blob_two = self.bucket.blob('blob_two.parquet')
        actual = self.bucket.list_blobs()
        expected = [blob_one, blob_two]
        self.assertEqual(actual, expected)


class FakeBlob(object):
    def __init__(self, name, bucket):
        self.name = name
        self._bucket = bucket  # type: FakeBucket
        self._exists = False
        self.__contents = io.BytesIO()

        self._create_if_not_exists()

    def create_resumable_upload_session(self):
        resumeable_upload_url = RESUMABLE_SESSION_URI_TEMPLATE % dict(
            bucket=self._bucket.name,
            upload_id=str(uuid.uuid4()),
        )
        upload = FakeBlobUpload(resumeable_upload_url, self)
        self._bucket.register_upload(upload)
        return resumeable_upload_url

    def delete(self):
        self._bucket.delete_blob(self)
        self._exists = False

    def download_as_bytes(self, start=0, end=None):
        # mimics Google's API by returning bytes
        # https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.download_as_bytes
        if end is None:
            end = self.__contents.tell()
        self.__contents.seek(start)
        return self.__contents.read(end - start)

    def exists(self, client=None):
        return self._exists

    def upload_from_string(self, data):
        # mimics Google's API by accepting bytes or str, despite the method name
        # https://googleapis.dev/python/storage/latest/blobs.html#google.cloud.storage.blob.Blob.upload_from_string
        if isinstance(data, str):
            data = bytes(data, 'utf8')
        self.__contents = io.BytesIO(data)
        self.__contents.seek(0, io.SEEK_END)

    def write(self, data):
        self.upload_from_string(data)

    @property
    def bucket(self):
        return self._bucket

    @property
    def size(self):
        if self.__contents.tell() == 0:
            return None
        return self.__contents.tell()

    def _create_if_not_exists(self):
        self._bucket.register_blob(self)
        self._exists = True


class FakeBlobTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.bucket = FakeBucket(self.client, 'test-bucket')

    def test_create_resumable_upload_session(self):
        blob = FakeBlob('fake-blob', self.bucket)
        resumable_upload_url = blob.create_resumable_upload_session()
        self.assertTrue(resumable_upload_url in self.client.uploads)

    def test_delete(self):
        blob = FakeBlob('fake-blob', self.bucket)
        blob.delete()
        self.assertFalse(blob.exists())
        self.assertEqual(self.bucket.list_blobs(), [])

    def test_upload_download(self):
        blob = FakeBlob('fake-blob', self.bucket)
        contents = b'test'
        blob.upload_from_string(contents)
        self.assertEqual(blob.download_as_bytes(), b'test')
        self.assertEqual(blob.download_as_bytes(start=2), b'st')
        self.assertEqual(blob.download_as_bytes(end=2), b'te')
        self.assertEqual(blob.download_as_bytes(start=2, end=3), b's')

    def test_size(self):
        blob = FakeBlob('fake-blob', self.bucket)
        self.assertEqual(blob.size, None)
        blob.upload_from_string(b'test')
        self.assertEqual(blob.size, 4)


class FakeCredentials(object):
    def __init__(self, client):
        self.client = client  # type: FakeClient

    def before_request(self, *args, **kwargs):
        pass


class FakeClient(object):
    def __init__(self, credentials=None):
        if credentials is None:
            credentials = FakeCredentials(self)
        self._credentials = credentials  # type: FakeCredentials
        self.uploads = OrderedDict()
        self.__buckets = OrderedDict()

    def bucket(self, bucket_id):
        try:
            return self.__buckets[bucket_id]
        except KeyError as e:
            raise google.cloud.exceptions.NotFound('Bucket %s not found' % bucket_id) from e

    def create_bucket(self, bucket_id):
        bucket = FakeBucket(self, bucket_id)
        return bucket

    def get_bucket(self, bucket_id):
        return self.bucket(bucket_id)

    def register_bucket(self, bucket):
        if bucket.name in self.__buckets:
            raise google.cloud.exceptions.Conflict('Bucket %s already exists' % bucket.name)
        self.__buckets[bucket.name] = bucket

    def delete_bucket(self, bucket):
        del self.__buckets[bucket.name]

    def register_upload(self, upload):
        self.uploads[upload.url] = upload


class FakeClientTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()

    def test_nonexistent_bucket(self):
        with self.assertRaises(google.cloud.exceptions.NotFound):
            self.client.bucket('test-bucket')

    def test_bucket(self):
        bucket_id = 'test-bucket'
        bucket = FakeBucket(self.client, bucket_id)
        actual = self.client.bucket(bucket_id)
        self.assertEqual(actual, bucket)

    def test_duplicate_bucket(self):
        bucket_id = 'test-bucket'
        FakeBucket(self.client, bucket_id)
        with self.assertRaises(google.cloud.exceptions.Conflict):
            FakeBucket(self.client, bucket_id)

    def test_create_bucket(self):
        bucket_id = 'test-bucket'
        bucket = self.client.create_bucket(bucket_id)
        actual = self.client.get_bucket(bucket_id)
        self.assertEqual(actual, bucket)


class FakeBlobUpload(object):
    def __init__(self, url, blob):
        self.url = url
        self.blob = blob  # type: FakeBlob
        self._finished = False
        self.__contents = io.BytesIO()

    def write(self, data):
        self.__contents.write(data)

    def finish(self):
        if not self._finished:
            self.__contents.seek(0)
            data = self.__contents.read()
            self.blob.upload_from_string(data)
            self._finished = True

    def terminate(self):
        self.blob.delete()
        self.__contents = None


class FakeResponse(object):
    def __init__(self, status_code=200, text=None):
        self.status_code = status_code
        self.text = text


class FakeAuthorizedSession(object):
    def __init__(self, credentials):
        self._credentials = credentials  # type: FakeCredentials

    def delete(self, upload_url):
        upload = self._credentials.client.uploads.pop(upload_url)
        upload.terminate()

    def put(self, url, data=None, headers=None):
        upload = self._credentials.client.uploads[url]

        if data is not None:
            if hasattr(data, 'read'):
                upload.write(data.read())
            else:
                upload.write(data)
        if not headers.get('Content-Range', '').endswith(smart_open.gcs._UNKNOWN):
            upload.finish()
            return FakeResponse(200)
        return FakeResponse(smart_open.gcs._UPLOAD_INCOMPLETE_STATUS_CODES[0])

    @staticmethod
    def _blob_with_url(url, client):
        # type: (str, FakeClient) -> FakeBlobUpload
        return client.uploads.get(url)


class FakeAuthorizedSessionTest(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.credentials = FakeCredentials(self.client)
        self.session = FakeAuthorizedSession(self.credentials)
        self.bucket = FakeBucket(self.client, 'test-bucket')
        self.blob = FakeBlob('test-blob', self.bucket)
        self.upload_url = self.blob.create_resumable_upload_session()

    def test_delete(self):
        self.session.delete(self.upload_url)
        self.assertFalse(self.blob.exists())
        self.assertDictEqual(self.client.uploads, {})

    def test_unfinished_put_does_not_write_to_blob(self):
        data = io.BytesIO(b'test')
        headers = {
            'Content-Range': 'bytes 0-3/*',
            'Content-Length': str(4),
        }
        response = self.session.put(self.upload_url, data, headers=headers)
        self.assertIn(response.status_code, smart_open.gcs._UPLOAD_INCOMPLETE_STATUS_CODES)
        self.session._blob_with_url(self.upload_url, self.client)
        blob_contents = self.blob.download_as_bytes()
        self.assertEqual(blob_contents, b'')

    def test_finished_put_writes_to_blob(self):
        data = io.BytesIO(b'test')
        headers = {
            'Content-Range': 'bytes 0-3/4',
            'Content-Length': str(4),
        }
        response = self.session.put(self.upload_url, data, headers=headers)
        self.assertEqual(response.status_code, 200)
        self.session._blob_with_url(self.upload_url, self.client)
        blob_contents = self.blob.download_as_bytes()
        data.seek(0)
        self.assertEqual(blob_contents, data.read())


if DISABLE_MOCKS:
    storage_client = google.cloud.storage.Client()
else:
    storage_client = FakeClient()
