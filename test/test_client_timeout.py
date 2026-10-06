import socket
import struct
import subprocess
import threading
import time

from kazoo.client import KazooClient

from test_client_response import (
    BIN,
    ZK_ADDRESS,
    SERVICE_PATH,
    MOCK_HOST,
    MOCK_PORT,
    make_response,
    make_frame,
    receive_request,
)


# 与 C++ 中的 kTcpTimeoutMs 对应。
TCP_TIMEOUT = 3.0

# 这是测试程序的保护时间，不是 RPC 超时。
PROCESS_TIMEOUT = 10


def run_case(name, behavior, expected, expect_timeout=True):
    stop = threading.Event()
    state = {}
    errors = []

    body = make_response(["TIMEOUT_TEST_OK"])
    frame = make_frame(body)

    def serve(listener):
        try:
            conn, _ = listener.accept()

            with conn:
                conn.settimeout(5)
                receive_request(conn)

                # 从模拟服务端收到完整请求开始计时，
                # 尽量排除 consumer 启动及 ZooKeeper 发现耗时。
                state["request_received"] = time.monotonic()

                def send(data):
                    try:
                        conn.sendall(data)
                        return True
                    except (BrokenPipeError, ConnectionResetError):
                        if expect_timeout:
                            # 客户端超时关闭连接后，这是允许的结果。
                            return False
                        raise

                if behavior == "silent":
                    # 接受请求后完全不响应。
                    stop.wait(8)

                elif behavior == "partial_length":
                    # 只发送四字节长度字段的一半。
                    send(frame[:2])
                    stop.wait(8)

                elif behavior == "partial_body":
                    # 发送完整长度字段和一个字节的响应体。
                    send(frame[:5])
                    stop.wait(8)

                elif behavior == "slow_drip":
                    # 先发送长度，随后每 0.4 秒发送一个字节。
                    # 每次间隔都小于 3 秒，但总时长超过 3 秒。
                    if not send(frame[:4]):
                        return

                    for byte in body:
                        if stop.wait(0.4):
                            return
                        if not send(bytes([byte])):
                            return

                    stop.wait(8)

                elif behavior == "shared_budget":
                    # 长度字段花掉约 2 秒。
                    if stop.wait(2):
                        return
                    if not send(frame[:4]):
                        return

                    # 响应体再等待 2 秒。
                    # 正确客户端应在总预算约 3 秒时退出，
                    # 而不是等到 4 秒后成功。
                    if stop.wait(2):
                        return
                    send(frame[4:])
                    stop.wait(8)

                elif behavior == "success":
                    # 在预算内正常返回。
                    if stop.wait(1):
                        return
                    send(frame)

                else:
                    raise ValueError(f"未知行为: {behavior}")

        except Exception as exc:
            errors.append(exc)

    with socket.socket() as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind((MOCK_HOST, MOCK_PORT))
        listener.listen(1)
        listener.settimeout(6)

        worker = threading.Thread(
            target=serve,
            args=(listener,),
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
                timeout=PROCESS_TIMEOUT,
            )
            finished = time.monotonic()
        except subprocess.TimeoutExpired:
            raise AssertionError(
                f"{name}: consumer 超过 {PROCESS_TIMEOUT} 秒未退出"
            )
        finally:
            stop.set()
            worker.join(timeout=7)

        if worker.is_alive():
            raise AssertionError(f"{name}: 模拟服务端未退出")

        if errors:
            raise AssertionError(f"{name}: 模拟服务端错误: {errors[0]}")

        if "request_received" not in state:
            raise AssertionError(f"{name}: consumer 未发送完整请求")

        output = result.stdout + result.stderr

        if result.returncode < 0:
            raise AssertionError(
                f"{name}: consumer 被信号终止\n{output[-2000:]}"
            )

        # 当前示例失败时可能返回 0，也可能被你改成了非零。
        # 通过具体错误输出判断预期的超时，而不是只看退出码。
        if expected not in output:
            raise AssertionError(
                f"{name}: 未找到预期输出 {expected!r}\n"
                f"{output[-2000:]}"
            )

        elapsed = finished - state["request_received"]

        if expect_timeout:
            if "rpc GetFriendsList response success!" in output:
                raise AssertionError(f"{name}: 超时调用被当成成功")

            # 本地测试使用宽松区间，容纳线程调度和进程退出清理。
            if not TCP_TIMEOUT - 0.8 <= elapsed <= TCP_TIMEOUT + 1.5:
                raise AssertionError(
                    f"{name}: 耗时 {elapsed:.2f}s 不在预期区间；"
                    "检查是否重置了超时预算，或退出清理过慢"
                )
        elif result.returncode != 0:
            raise AssertionError(
                f"{name}: 正常调用退出码异常\n{output[-2000:]}"
            )

        print(f"PASS {name}: {elapsed:.2f}s")


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

        run_case(
            "完全不响应",
            "silent",
            "read response length timeout",
        )

        run_case(
            "长度字段只发送一半",
            "partial_length",
            "read response length timeout",
        )

        run_case(
            "响应体只发送一部分",
            "partial_body",
            "read response body timeout",
        )

        run_case(
            "持续慢速发送",
            "slow_drip",
            "read response body timeout",
        )

        run_case(
            "长度和响应体共享时间预算",
            "shared_budget",
            "read response body timeout",
        )

        run_case(
            "预算内正常返回",
            "success",
            "TIMEOUT_TEST_OK",
            expect_timeout=False,
        )

        print("全部接收超时测试通过")

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