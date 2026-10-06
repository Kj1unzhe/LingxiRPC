#include "mprpcchannel.h"
#include <string>
#include "rpcheader.pb.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <unistd.h>
#include <errno.h>
#include "mprpcapplication.h"
#include "mprpccontroller.h"
#include "zookeeperutil.h"
#include <cstdint>
#include <cstring>
#include <fcntl.h>
#include <poll.h>
#include <chrono>
#include <cerrno>

namespace
{

    constexpr uint32_t kMaxResponseBytes = 16 * 1024 * 1024;

    // 一个对象独占一个 socket。
    // 无论正常结束还是提前 return，都会自动关闭。
    class SocketGuard
    {
    public:
        explicit SocketGuard(int fd) : fd_(fd) {}

        ~SocketGuard()
        {
            if (fd_ >= 0)
            {
                ::close(fd_);
            }
        }

        SocketGuard(const SocketGuard &) = delete;
        SocketGuard &operator=(const SocketGuard &) = delete;

    private:
        int fd_;
    };

    // 确保整个字符串都发送出去。
    using Clock = std::chrono::steady_clock;
    using Deadline = Clock::time_point;

    constexpr int kTcpTimeoutMs = 3000;

    // 检查整个 TCP 调用的时间预算是否用完。
    bool CheckDeadline(
        const Deadline &deadline,
        const char *stage,
        std::string &error)
    {
        if (Clock::now() >= deadline)
        {
            error = std::string(stage) + " timeout";
            return false;
        }

        return true;
    }

    // 非阻塞 socket 暂时无法读写时，等待对应事件。
    bool WaitForEvent(
        int fd,
        short events,
        const Deadline &deadline,
        const char *stage,
        std::string &error)
    {
        for (;;)
        {
            const auto now = Clock::now();

            if (now >= deadline)
            {
                error = std::string(stage) + " timeout";
                return false;
            }

            // poll 使用毫秒。剩余不足 1 ms 时至少等待 1 ms，
            // 避免 poll(..., 0) 反复立即返回。
            auto remaining_ms =
                std::chrono::duration_cast<std::chrono::milliseconds>(
                    deadline - now)
                    .count();

            if (remaining_ms < 1)
            {
                remaining_ms = 1;
            }

            // 本轮预算固定为 3000 ms，转换为 int 不会溢出。
            pollfd pfd{};
            pfd.fd = fd;
            pfd.events = events;

            const int result = ::poll(
                &pfd, 1, static_cast<int>(remaining_ms));

            if (result == 0)
            {
                // 再次检查真实截止时间，避免毫秒取整造成提前超时。
                continue;
            }

            if (result < 0)
            {
                if (errno == EINTR)
                {
                    continue;
                }

                const int saved_errno = errno;
                error = std::string(stage) + " poll failed: " + std::strerror(saved_errno);
                return false;
            }

            if (!CheckDeadline(deadline, stage, error))
            {
                return false;
            }

            if (pfd.revents & POLLNVAL)
            {
                error = std::string(stage) + ": invalid socket";
                return false;
            }

            // HUP/ERR 不直接当成成功或失败：
            // 交给 recv/send/getsockopt 判断实际结果。
            // 特别是 HUP 出现时，缓冲区可能还有尚未读取的数据。
            if (pfd.revents & (events | POLLERR | POLLHUP))
            {
                return true;
            }
        }
    }

    bool SetNonBlocking(int fd, std::string &error)
    {
        const int flags = ::fcntl(fd, F_GETFL, 0);

        if (flags == -1)
        {
            const int saved_errno = errno;
            error = std::string("get socket flags failed: ") + std::strerror(saved_errno);
            return false;
        }

        if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
        {
            const int saved_errno = errno;
            error = std::string("set nonblocking failed: ") + std::strerror(saved_errno);
            return false;
        }

        return true;
    }

    bool ConnectWithDeadline(
        int fd,
        const sockaddr_in &address,
        const Deadline &deadline,
        std::string &error)
    {
        if (!CheckDeadline(deadline, "connect", error))
        {
            return false;
        }

        const int result = ::connect(
            fd,
            reinterpret_cast<const sockaddr *>(&address),
            sizeof(address));

        if (result == 0)
        {
            return CheckDeadline(deadline, "connect", error);
        }

        const int saved_errno = errno;

        // 非阻塞连接通常先返回 EINPROGRESS。
        if (saved_errno != EINPROGRESS &&
            saved_errno != EALREADY &&
            saved_errno != EINTR)
        {
            error = std::string("connect failed: ") + std::strerror(saved_errno);
            return false;
        }

        if (!WaitForEvent(fd, POLLOUT, deadline, "connect", error))
        {
            return false;
        }

        // 可写不等于连接成功，必须读取 SO_ERROR。
        int socket_error = 0;
        socklen_t length = sizeof(socket_error);

        if (::getsockopt(
                fd,
                SOL_SOCKET,
                SO_ERROR,
                &socket_error,
                &length) == -1)
        {
            const int get_error = errno;
            error = std::string("get connect result failed: ") + std::strerror(get_error);
            return false;
        }

        if (socket_error != 0)
        {
            error = std::string("connect failed: ") + std::strerror(socket_error);
            return false;
        }

        return CheckDeadline(deadline, "connect", error);
    }

    bool SendAll(
        int fd,
        const std::string &data,
        const Deadline &deadline,
        std::string &error)
    {
        size_t sent = 0;

        while (sent < data.size())
        {
            if (!CheckDeadline(deadline, "send request", error))
            {
                return false;
            }

            const ssize_t n = ::send(
                fd,
                data.data() + sent,
                data.size() - sent,
                MSG_NOSIGNAL);

            if (n > 0)
            {
                sent += static_cast<size_t>(n);
                continue;
            }

            if (n == 0)
            {
                error = "send request made no progress";
                return false;
            }

            const int saved_errno = errno;

            if (saved_errno == EINTR)
            {
                continue;
            }

            if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
            {
                if (!WaitForEvent(
                        fd, POLLOUT, deadline, "send request", error))
                {
                    return false;
                }
                continue;
            }

            error = std::string("send request failed: ") + std::strerror(saved_errno);
            return false;
        }

        return CheckDeadline(deadline, "send request", error);
    }

    bool RecvExact(
        int fd,
        char *data,
        size_t size,
        const Deadline &deadline,
        const char *stage,
        std::string &error)
    {
        size_t received = 0;

        while (received < size)
        {
            if (!CheckDeadline(deadline, stage, error))
            {
                return false;
            }

            const ssize_t n = ::recv(
                fd,
                data + received,
                size - received,
                0);

            if (n > 0)
            {
                received += static_cast<size_t>(n);
                continue;
            }

            if (n == 0)
            {
                error = std::string(stage) + ": connection closed after receiving " + std::to_string(received) + " of " + std::to_string(size) + " bytes";
                return false;
            }

            const int saved_errno = errno;

            if (saved_errno == EINTR)
            {
                continue;
            }

            if (saved_errno == EAGAIN || saved_errno == EWOULDBLOCK)
            {
                if (!WaitForEvent(fd, POLLIN, deadline, stage, error))
                {
                    return false;
                }
                continue;
            }

            error = std::string(stage) + " failed: " + std::strerror(saved_errno);
            return false;
        }

        return CheckDeadline(deadline, stage, error);
    }

} // namespace

/*
header_size + service_name method_name args_size + args
*/
// 所有通过stub代理对象调用的rpc方法，都走到这里了，统一做rpc方法调用的数据数据序列化和网络发送
void MprpcChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                              google::protobuf::RpcController *controller,
                              const google::protobuf::Message *request,
                              google::protobuf::Message *response,
                              google::protobuf::Closure *done)
{
    const google::protobuf::ServiceDescriptor *sd = method->service();
    std::string service_name = sd->name();    // service_name
    std::string method_name = method->name(); // method_name

    // 获取参数的序列化字符串长度 args_size
    uint32_t args_size = 0;
    std::string args_str;
    if (request->SerializeToString(&args_str))
    {
        args_size = args_str.size();
    }
    else
    {
        controller->SetFailed("serialize request error!");
        return;
    }

    // 定义rpc的请求header
    mprpc::RpcHeader rpcHeader;
    rpcHeader.set_service_name(service_name);
    rpcHeader.set_method_name(method_name);
    rpcHeader.set_args_size(args_size);

    uint32_t header_size = 0;
    std::string rpc_header_str;
    if (rpcHeader.SerializeToString(&rpc_header_str))
    {
        header_size = rpc_header_str.size();
    }
    else
    {
        controller->SetFailed("serialize rpc header error!");
        return;
    }

    // 组织待发送的rpc请求的字符串
    std::string send_rpc_str;
    send_rpc_str.insert(0, std::string((char *)&header_size, 4)); // header_size
    send_rpc_str += rpc_header_str;                               // rpcheader
    send_rpc_str += args_str;                                     // args

    // 打印调试信息
    std::cout << "============================================" << std::endl;
    std::cout << "header_size: " << header_size << std::endl;
    std::cout << "rpc_header_str: " << rpc_header_str << std::endl;
    std::cout << "service_name: " << service_name << std::endl;
    std::cout << "method_name: " << method_name << std::endl;
    std::cout << "args_str: " << args_str << std::endl;
    std::cout << "============================================" << std::endl;

    // 使用tcp编程，完成rpc方法的远程调用
    int clientfd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (clientfd == -1)
    {
        const int saved_errno = errno;

        controller->SetFailed(
            std::string("create socket failed: ") + std::strerror(saved_errno));

        return;
    }

    SocketGuard socket_guard(clientfd);

    // 读取配置文件rpcserver的信息
    // std::string ip = MprpcApplication::GetInstance().GetConfig().Load("rpcserverip");
    // uint16_t port = atoi(MprpcApplication::GetInstance().GetConfig().Load("rpcserverport").c_str());
    // rpc调用方想调用service_name的method_name服务，需要查询zk上该服务所在的host信息
    ZkClient zkCli;
    zkCli.Start();
    //  /UserServiceRpc/Login
    std::string method_path = "/" + service_name + "/" + method_name;
    // 127.0.0.1:8000
    std::string host_data = zkCli.GetData(method_path.c_str());
    if (host_data == "")
    {
        controller->SetFailed(method_path + " is not exist!");
        return;
    }
    int idx = host_data.find(":");
    if (idx == -1)
    {
        controller->SetFailed(method_path + " address is invalid!");
        return;
    }
    std::string ip = host_data.substr(0, idx);
    uint16_t port = atoi(host_data.substr(idx + 1, host_data.size() - idx).c_str());

    struct sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(ip.c_str());

    // 连接 RPC 服务节点。

    std::string error;

    // socket 创建成功后已由 SocketGuard 管理。
    // 这里改成非阻塞，让连接、发送、接收都可以控制等待时间。
    if (!SetNonBlocking(clientfd, error))
    {
        controller->SetFailed(error);
        return;
    }

    // 服务发现已完成，从这里开始计算 TCP 调用时间预算。
    // 注意：整个过程只创建一次 deadline。
    const Deadline deadline =
        Clock::now() + std::chrono::milliseconds(kTcpTimeoutMs);

    // 1. 限时建立连接。
    if (!ConnectWithDeadline(
            clientfd, server_addr, deadline, error))
    {
        controller->SetFailed(error);
        return;
    }

    // 2. 限时发送完整请求。
    if (!SendAll(clientfd, send_rpc_str, deadline, error))
    {
        controller->SetFailed(error);
        return;
    }

    // 3. 限时读取四字节响应长度。
    uint32_t network_size = 0;

    if (!RecvExact(
            clientfd,
            reinterpret_cast<char *>(&network_size),
            sizeof(network_size),
            deadline,
            "read response length",
            error))
    {
        controller->SetFailed(error);
        return;
    }

    const uint32_t response_size = ntohl(network_size);

    if (response_size > kMaxResponseBytes)
    {
        controller->SetFailed(
            "response too large: " + std::to_string(response_size));
        return;
    }

    // 4. 使用剩余时间读取完整响应体。
    std::string response_str(response_size, '\0');

    if (response_size > 0 &&
        !RecvExact(
            clientfd,
            &response_str[0],
            response_size,
            deadline,
            "read response body",
            error))
    {
        controller->SetFailed(error);
        return;
    }

    // 5. 接收完成后解析响应。
    if (!CheckDeadline(deadline, "parse response", error))
    {
        controller->SetFailed(error);
        return;
    }

    if (!response->ParseFromString(response_str))
    {
        controller->SetFailed(
            "parse response failed, size: " + std::to_string(response_size));
        return;
    }

    // 解析期间不能被这个机制中断，但完成后检查是否超出预算。
    if (!CheckDeadline(deadline, "parse response", error))
    {
        controller->SetFailed(error);
        return;
    }

    // 自动析构 SocketGuard，关闭 socket。
}