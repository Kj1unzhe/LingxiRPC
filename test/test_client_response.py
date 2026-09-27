import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

from kazoo.client import KazooClient


ROOT = Path(__file__).resolve().parents[1]
BIN = ROOT / "bin"

ZK_ADDRESS = "127.0.0.1:2181"
SERVICE_PATH = "/FiendServiceRpc/GetFriendsList"

MOCK_HOST = "127.0.0.1"
MOCK_PORT = 18080

MAX_RESPONSE = 16 * 1024 * 1024


# ---------- 编码合法的 Protobuf 响应 ----------

def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def bytes_field(number, value):
    return (
        varint((number << 3) | 2)
        + varint(len(value))
        + value
    )


def make_response(names):
    # GetFriendsListResponse:
    #   field 1: ResultCode，空消息表示默认 errcode=0
    #   field 2: repeated bytes friends
    result = bytes_field(1, b"")
    for name in names:
        result += bytes_field(2, name.encode("utf-8"))
    return result


def make_frame(body):
    # 新响应协议的长度前缀使用网络字节序。
    return struct.pack("!I", len(body)) + body


# ---------- 接收 consumer 发来的原有请求 ----------

def recv_exact(conn, size):
    result = bytearray()
    while len(result) < size:
        data = conn.recv(size - len(result))
        if not data:
            raise RuntimeError("consumer 提前断开")
        result.extend(data)
    return bytes(result)


def read_varint(data, offset):
    value = 0
    shift = 0

    while offset < len(data) and shift < 70:
        byte = data[offset]
        offset += 1
        value |= (byte & 0x7F) << shift

        if byte < 128:
            return value, offset

        shift += 7

    raise RuntimeError("非法 varint")


def read_args_size(header):
    offset = 0
    args_size = 0

    while offset < len(header):
        tag, offset = read_varint(header, offset)
        field = tag >> 3
        wire_type = tag & 7

        if wire_type == 0:
            value, offset = read_varint(header, offset)
            if field == 3:
                args_size = value
        elif wire_type == 2:
            length, offset = read_varint(header, offset)
            offset += length
            if offset > len(header):
                raise RuntimeError("请求头字段越界")
        else:
            raise RuntimeError("测试脚本不支持此请求头字段类型")

    return args_size


def receive_request(conn):
    # 现有请求长度仍使用主机字节序。
    header_size = struct.unpack("=I", recv_exact(conn, 4))[0]

    if not 0 < header_size <= 64 * 1024:
        raise RuntimeError("请求头长度异常")

    header = recv_exact(conn, header_size)
    args_size = read_args_size(header)

    if args_size > MAX_RESPONSE:
        raise RuntimeError("请求体过大")

    recv_exact(conn, args_size)


# ---------- 模拟不同的响应行为 ----------

def serve_once(listener, parts, delay, hold_open, errors):
    try:
        conn, _ = listener.accept()
        with conn:
            conn.settimeout(5)
            receive_request(conn)

            for index, part in enumerate(parts):
                conn.sendall(part)
                if index + 1 < len(parts):
                    time.sleep(delay)

            # 部分测试保持连接打开，确认客户端不依赖 EOF。
            if hold_open:
                try:
                    data = conn.recv(1)
                    if data:
                        raise RuntimeError("收到意外的额外请求")
                except ConnectionResetError:
                    pass
    except Exception as exc:
        errors.append(exc)


def run_case(name, parts, expected, delay=0, hold_open=False):
    errors = []

    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((MOCK_HOST, MOCK_PORT))
        listener.listen(1)
        listener.settimeout(6)

        worker = threading.Thread(
            target=serve_once,
            args=(listener, parts, delay, hold_open, errors),
            daemon=True,
        )
        worker.start()

        try:
            result = subprocess.run(
                [str(BIN / "consumer"), "-i", "test.conf"],
                cwd=BIN,
                capture_output=True,
                text=True,
                errors="replace",
                timeout=5,
            )
        except subprocess.TimeoutExpired:
            raise AssertionError(f"{name}: consumer 超时")
        finally:
            worker.join(timeout=7)

        if worker.is_alive():
            raise AssertionError(f"{name}: 模拟服务端未退出")

        if errors:
            raise AssertionError(f"{name}: 模拟服务端错误: {errors[0]}")

        output = result.stdout + result.stderr

        if result.returncode != 0:
            raise AssertionError(
                f"{name}: consumer 异常退出\n{output[-2000:]}"
            )

        # 当前好友示例即使 RPC 失败，也返回退出码 0，
        # 因此必须检查实际输出，不能只看 returncode。
        if expected not in output:
            raise AssertionError(
                f"{name}: 未找到预期输出 {expected!r}\n"
                f"{output[-2000:]}"
            )

        print(f"PASS {name}")


def main():
    zk = KazooClient(hosts=ZK_ADDRESS, timeout=5)
    original = None
    redirected = False

    try:
        zk.start(timeout=5)

        original, stat = zk.get(SERVICE_PATH)
        print("原服务地址:", original.decode())

        zk.set(
            SERVICE_PATH,
            f"{MOCK_HOST}:{MOCK_PORT}".encode(),
            version=stat.version,
        )
        redirected = True

        body = make_response(["MOCK_OK"])
        frame = make_frame(body)

        run_case(
            "普通带长度响应",
            [frame],
            "MOCK_OK",
        )

        run_case(
            "四字节长度分片",
            [frame[:1], frame[1:3], frame[3:4], frame[4:]],
            "MOCK_OK",
            delay=0.2,
        )

        run_case(
            "响应体逐字节发送",
            [frame[:4]] + [bytes([b]) for b in body],
            "MOCK_OK",
            delay=0.03,
        )

        names = [f"friend_{i:04d}" for i in range(600)]
        large_body = make_response(names)
        assert len(large_body) > 1024

        large_frame = make_frame(large_body)
        chunks = [
            large_frame[i:i + 257]
            for i in range(0, len(large_frame), 257)
        ]

        run_case(
            f"大响应，响应体 {len(large_body)} 字节",
            chunks,
            "friend_0599",
            delay=0.01,
        )

        run_case(
            "响应完整后服务端保持连接",
            [frame],
            "MOCK_OK",
            hold_open=True,
        )

        run_case(
            "长度字段只发送两字节就断开",
            [frame[:2]],
            "read response length failed:",
        )

        run_case(
            "响应体未发送完整就断开",
            [frame[:-1]],
            "read response body failed:",
        )

        run_case(
            "响应长度超过上限",
            [struct.pack("!I", MAX_RESPONSE + 1)],
            "response too large:",
            hold_open=True,
        )

        run_case(
            "完整但非法的 Protobuf",
            [make_frame(b"\x80")],
            "parse response failed",
        )

        run_case(
            "零长度的合法 Protobuf 响应",
            [make_frame(b"")],
            "rpc GetFriendsList response success!",
        )

        print("全部客户端响应测试通过")

    finally:
        try:
            if redirected:
                zk.set(SERVICE_PATH, original)
                print("已恢复服务地址:", original.decode())
        finally:
            zk.stop()
            zk.close()


if __name__ == "__main__":
    main()