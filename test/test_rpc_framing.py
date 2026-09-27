import socket
import struct

HOST = "127.0.0.1"
PORT = 8000


# 手工编码本测试需要的 Protobuf 字段，无需安装 protobuf 包。
def varint(value):
    result = bytearray()
    while value >= 128:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def bytes_field(number, value):
    return varint((number << 3) | 2) + varint(len(value)) + value


def make_header(args_size):
    # 名称与项目 friend.proto 保持一致：FiendServiceRpc。
    return (
        bytes_field(1, b"FiendServiceRpc")
        + bytes_field(2, b"GetFriendsList")
        + varint(3 << 3)
        + varint(args_size)
    )


# GetFriendsListRequest { userid: 1000 }
args = varint(1 << 3) + varint(1000)
header = make_header(len(args))

# 当前项目长度前缀使用主机字节序，与现有客户端保持一致。
packet = struct.pack("=I", len(header)) + header + args
body_offset = 4 + len(header)


def call(parts):
    """分段发送，检查未完成时无响应，完成后收集完整响应。"""
    with socket.create_connection((HOST, PORT), timeout=3) as sock:
        for index, part in enumerate(parts):
            sock.sendall(part)

            if index < len(parts) - 1:
                # 给服务端处理不完整数据的时间。
                sock.settimeout(0.15)
                try:
                    data = sock.recv(1)
                except socket.timeout:
                    pass  # 正确：服务端仍在等待剩余数据。
                else:
                    raise AssertionError(
                        f"请求尚未完整，服务端已响应或关闭连接: {data!r}"
                    )

        # 当前服务端发送响应后关闭发送方向，读到 EOF 为止。
        sock.settimeout(3)
        response = bytearray()
        while True:
            data = sock.recv(4096)
            if not data:
                break
            response.extend(data)

        assert response, "完整请求没有获得响应"
        return bytes(response)


def expect_closed(data):
    """非法报文应被关闭连接，不能一直等待。"""
    with socket.create_connection((HOST, PORT), timeout=3) as sock:
        sock.settimeout(3)
        try:
            sock.sendall(data)
            reply = sock.recv(1)
        except (ConnectionResetError, BrokenPipeError):
            return  # 主动断开也可能表现为连接重置。

        assert reply == b"", f"非法报文收到了意外响应: {reply!r}"
        # socket.timeout 会直接使测试失败。


def main():
    baseline = call([packet])
    for name in (b"gao yang", b"liu hong", b"wang shuo"):
        assert name in baseline, f"普通调用缺少预期好友: {name!r}"
    print("PASS 普通请求基线")

    cases = [
        ("长度字段分片", [packet[:1], packet[1:4], packet[4:]]),
        ("协议头分片", [packet[:7], packet[7:body_offset],
                        packet[body_offset:]]),
        ("请求体分片", [packet[:body_offset + 1],
                        packet[body_offset + 1:]]),
        ("逐字节发送", [packet[i:i + 1] for i in range(len(packet))]),
    ]

    for name, parts in cases:
        assert call(parts) == baseline, f"{name}: 响应与基线不一致"
        print(f"PASS {name}")

    oversized_args_header = make_header(16 * 1024 * 1024 + 1)
    invalid_cases = [
        ("零长度协议头", struct.pack("=I", 0)),
        ("协议头超限", struct.pack("=I", 64 * 1024 + 1)),
        ("损坏的协议头", struct.pack("=I", 1) + b"\x80"),
        ("请求体长度超限",
         struct.pack("=I", len(oversized_args_header))
         + oversized_args_header),
    ]

    for name, data in invalid_cases:
        expect_closed(data)
        # 检查非法请求没有让整个服务端退出。
        assert call([packet]) == baseline, f"{name}: 后续正常调用失败"
        print(f"PASS {name}，且服务端仍可正常调用")

    print("全部测试通过")


if __name__ == "__main__":
    main()