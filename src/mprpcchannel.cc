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
    bool SendAll(int fd, const std::string &data, std::string &error)
    {
        size_t sent = 0;

        while (sent < data.size())
        {
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

            if (n < 0 && errno == EINTR)
            {
                continue;
            }

            if (n == 0)
            {
                error = "send made no progress";
            }
            else
            {
                const int saved_errno = errno;
                error = std::string("send failed: ") + std::strerror(saved_errno);
            }

            return false;
        }

        return true;
    }

    // 确保恰好收到 size 字节。
    // 提前断开或发生其他错误时返回 false。
    bool RecvExact(
        int fd,
        char *data,
        size_t size,
        std::string &error)
    {
        size_t received = 0;

        while (received < size)
        {
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

            if (n < 0 && errno == EINTR)
            {
                continue;
            }

            if (n == 0)
            {
                error = "connection closed after receiving " + std::to_string(received) + " of " + std::to_string(size) + " bytes";
            }
            else
            {
                const int saved_errno = errno;
                error = std::string("recv failed: ") + std::strerror(saved_errno);
            }

            return false;
        }

        return true;
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
    if (::connect(
            clientfd,
            reinterpret_cast<sockaddr *>(&server_addr),
            sizeof(server_addr)) == -1)
    {
        const int saved_errno = errno;

        controller->SetFailed(
            std::string("connect failed: ") + std::strerror(saved_errno));

        return;
    }

    std::string error;

    // 1. 完整发送请求。
    if (!SendAll(clientfd, send_rpc_str, error))
    {
        controller->SetFailed(error);
        return;
    }

    // 2. 先读满四字节响应长度。
    uint32_t network_size = 0;

    if (!RecvExact(
            clientfd,
            reinterpret_cast<char *>(&network_size),
            sizeof(network_size),
            error))
    {
        controller->SetFailed(
            "read response length failed: " + error);
        return;
    }

    // 将网络字节序转换成本机整数。
    const uint32_t response_size = ntohl(network_size);

    // 3. 分配内存之前，先检查长度是否合法。
    if (response_size > kMaxResponseBytes)
    {
        controller->SetFailed(
            "response too large: " + std::to_string(response_size));
        return;
    }

    // 4. 根据响应长度分配缓冲区。
    std::string response_str(response_size, '\0');

    // 空响应体合法，避免对空字符串使用 &response_str[0]。
    if (response_size > 0 &&
        !RecvExact(
            clientfd,
            &response_str[0],
            response_size,
            error))
    {
        controller->SetFailed(
            "read response body failed: " + error);
        return;
    }

    // 5. 完整收到响应之后再解析。
    if (!response->ParseFromString(response_str))
    {
        controller->SetFailed(
            "parse response failed, size: " + std::to_string(response_size));
        return;
    }

    // 函数结束时，socket_guard 自动关闭 socket。
}