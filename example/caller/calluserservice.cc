#include <iostream>
#include "mprpcapplication.h"
#include "mprpcchannel.h"
#include "mprpccontroller.h"
#include "user.pb.h"

int main(int argc, char** argv)
{
    MprpcApplication::Init(argc, argv);

    // Channel 生命周期覆盖 Stub 的使用过程。
    MprpcChannel channel;
    fixbug::UserServiceRpc_Stub stub(&channel);

    // 用于记录 RPC 调用过程中的错误。
    MprpcController controller;

    // ---------- 登录调用 ----------
    fixbug::LoginRequest request;
    request.set_name("zhang san");
    request.set_pwd("123456");

    fixbug::LoginResponse response;

    // 第一个参数原来是 nullptr，现在传 controller 的地址。
    stub.Login(&controller, &request, &response, nullptr);

    // 先检查网络通信、序列化等框架层错误。
    if (controller.Failed()) {
        std::cerr << "Login RPC failed: "
                  << controller.ErrorText() << std::endl;
        return 1;
    }

    // RPC 正常完成之后，才检查业务结果。
    if (response.result().errcode() == 0) {
        std::cout << "rpc login response success: "
                  << response.sucess() << std::endl;
    } else {
        std::cout << "rpc login response error: "
                  << response.result().errmsg() << std::endl;
    }

    // ---------- 注册调用 ----------
    fixbug::RegisterRequest req;
    req.set_id(2000);
    req.set_name("mprpc");
    req.set_pwd("666666");

    fixbug::RegisterResponse rsp;

    // 复用 controller 前，清空上一次调用的错误状态。
    controller.Reset();

    stub.Register(&controller, &req, &rsp, nullptr);

    if (controller.Failed()) {
        std::cerr << "Register RPC failed: "
                  << controller.ErrorText() << std::endl;
        return 1;
    }

    if (rsp.result().errcode() == 0) {
        std::cout << "rpc register response success: "
                  << rsp.sucess() << std::endl;
    } else {
        std::cout << "rpc register response error: "
                  << rsp.result().errmsg() << std::endl;
    }

    return 0;
}