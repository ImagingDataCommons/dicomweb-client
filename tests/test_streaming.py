"""Streaming multipart decode of a single large part must stay O(n)."""

import time

import pytest

from dicomweb_client.web import DICOMwebClient


CONTENT_TYPE = (
    'multipart/related; type="application/dicom"; boundary="boundary"'
)
MAX_STREAM_TO_BUFFERED_RATIO = 2


@pytest.mark.parametrize(
    'payload_size,chunk_size',
    [
        pytest.param(
            20 * 10**6, 32 * 10**3, id='20MB-payload-32KB-chunk'
        ),
        pytest.param(
            40 * 10**6, 32 * 10**3, id='40MB-payload-32KB-chunk'
        ),
    ],
)
def test_streaming_single_part_decode_is_linear(
    httpserver, payload_size, chunk_size
):
    """Streaming a single large part should match buffered time to O(n).

    Serves a real HTTP multipart/related body with one part (the DBT
    shape). ``stream=False`` is the ``retrieve_instance`` path;
    ``stream=True`` is the ``iter_series`` path. Both must return the
    same payload, and streaming must not be much slower.

    """
    payload = b'\x00' * payload_size
    message = DICOMwebClient._encode_multipart_message(
        content=[payload],
        content_type=CONTENT_TYPE,
    )
    httpserver.serve_content(
        content=message,
        code=200,
        headers={'content-type': CONTENT_TYPE},
    )
    client = DICOMwebClient(httpserver.url, chunk_size=chunk_size)
    url = f'{httpserver.url}/studies/1.2.3/series/1.2.4'

    start = time.perf_counter()
    buffered = list(client._http_get_multipart(url, stream=False))
    t_buffered = time.perf_counter() - start

    start = time.perf_counter()
    streamed = list(client._http_get_multipart(url, stream=True))
    t_streamed = time.perf_counter() - start

    assert buffered == [payload]
    assert streamed == [payload]

    ratio = (
        t_streamed / t_buffered if t_buffered > 0 else float('inf')
    )
    print(
        f'streamed {t_streamed:.3f}s vs buffered {t_buffered:.3f}s '
        f'({ratio:.1f}x)'
    )
    assert ratio < MAX_STREAM_TO_BUFFERED_RATIO, (
        f'streaming decode was {ratio:.1f}x slower than buffered '
        f'({t_streamed:.3f}s vs {t_buffered:.3f}s); '
        f'expected <{MAX_STREAM_TO_BUFFERED_RATIO}x for O(n) scaling'
    )


def _stream_parts(httpserver, payloads, chunk_size):
    message = DICOMwebClient._encode_multipart_message(
        content=payloads,
        content_type=CONTENT_TYPE,
    )
    httpserver.serve_content(
        content=message,
        code=200,
        headers={'content-type': CONTENT_TYPE},
    )
    client = DICOMwebClient(httpserver.url, chunk_size=chunk_size)
    url = f'{httpserver.url}/studies/1.2.3/series/1.2.4'
    return list(client._http_get_multipart(url, stream=True))


def test_delimiter_split_across_chunks(httpserver):
    payload = b'part-bytes'
    message = DICOMwebClient._encode_multipart_message(
        content=[payload],
        content_type=CONTENT_TYPE,
    )
    delimiter = b'\r\n--boundary'
    closing = message.rfind(delimiter)
    chunk_size = closing + len(delimiter) // 2
    assert 0 < chunk_size < len(message)
    assert _stream_parts(httpserver, [payload], chunk_size) == [payload]


def test_multiple_parts_chunked(httpserver):
    payloads = [b'one', b'two', b'three']
    assert _stream_parts(httpserver, payloads, chunk_size=5) == payloads
