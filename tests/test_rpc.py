"""Tests for the newline-framed JSON RPC codec (issue #2 transport)."""

import io

from hermes_workflows import rpc


def test_round_trip_frame():
    buf = io.StringIO()
    rpc.write_frame(buf, {"t": "call", "id": 1, "method": "log", "params": {"message": "hi"}})
    buf.seek(0)
    frame = rpc.read_frame(buf)
    assert frame == {"t": "call", "id": 1, "method": "log", "params": {"message": "hi"}}


def test_encode_frame_is_single_line():
    line = rpc.encode_frame({"t": "done", "ok": True, "value": {"a": [1, 2, 3]}})
    assert "\n" not in line


def test_read_frame_returns_none_at_eof():
    assert rpc.read_frame(io.StringIO("")) is None


def test_truncated_frame_raises():
    # A line without a terminating newline means the peer died mid-write.
    try:
        rpc.read_frame(io.StringIO('{"t":"call"}'))
    except rpc.RPCProtocolError:
        return
    raise AssertionError("expected RPCProtocolError on truncated frame")


def test_malformed_json_raises():
    try:
        rpc.read_frame(io.StringIO("not json\n"))
    except rpc.RPCProtocolError:
        return
    raise AssertionError("expected RPCProtocolError on malformed JSON")


def test_non_object_frame_raises():
    try:
        rpc.read_frame(io.StringIO("[1,2,3]\n"))
    except rpc.RPCProtocolError:
        return
    raise AssertionError("expected RPCProtocolError on non-object frame")


def test_frame_without_type_tag_raises():
    try:
        rpc.read_frame(io.StringIO('{"id":1}\n'))
    except rpc.RPCProtocolError:
        return
    raise AssertionError("expected RPCProtocolError on missing type tag")


def test_channel_send_recv():
    out = io.StringIO()
    chan = rpc.Channel(reader=io.StringIO('{"t":"ret","id":1,"ok":true,"value":42}\n'), writer=out)
    chan.send({"t": "call", "id": 1, "method": "agent", "params": {}})
    assert "agent" in out.getvalue()
    assert chan.recv() == {"t": "ret", "id": 1, "ok": True, "value": 42}
