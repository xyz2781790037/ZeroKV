#pragma once

#include <cstddef>
#include <cstdint>
#include <mutex>
#include <string>

// ShmAllocation 描述了一块从共享内存池中切分出来的物理切片。
// 这个结构体扮演了“跨进程寻址凭证”的角色。
struct ShmAllocation {
    std::string shm_name;  // 所属的底层共享内存文件名（如 "/kvcache_pool_1"）

    // 【核心参数】：跨进程寻址坐标
    // Go 进程拿到这个 offset 后，直接在它自己 mmap 的内存基址上加上这个偏移量，
    // 就能零拷贝地读到 C++ 刚刚写下的数据。
    uint64_t offset = 0;

    uint64_t length = 0;  // 数据的实际物理长度
    uint32_t checksum =
        0;  // 数据的 CRC32 校验和，防跨进程传输时发生静默内存损坏

    // 【进程内专属】：当前 C++ 进程的用户态指针
    // 指向这块内存的真实虚拟内存地址。注意：这个指针绝对不能发给 Go 进程！
    // 因为跨进程后虚拟内存空间是隔离的，发指针过去会导致 Go 进程段错误。
    void* data = nullptr;
};

// ShmAllocator：数据面的基石。
// 架构本质：一个线程安全的、基于 mmap 的线性分配器（Bump-pointer Allocator）。
class ShmAllocator {
   public:
    // 构造函数只记录配置参数，不涉及实际的 OS 系统调用。
    ShmAllocator(std::string shm_name, uint64_t size);

    // 析构函数应负责 munmap 解除映射，以及可能的 shm_unlink 清理内核对象。
    ~ShmAllocator();

    // 禁用拷贝语义，独占底层的 File Descriptor (fd_) 和内存地址 (base_)
    ShmAllocator(const ShmAllocator&) = delete;
    ShmAllocator& operator=(const ShmAllocator&) = delete;

    // 真正的物理挂载动作。
    // 底层应调用：shm_open() -> ftruncate() -> mmap()。
    bool Init();

    // 主动断开并清理映射。
    void Close();

    // Allocate 仅仅是移动游标（cursor_ += length），不拷贝数据。
    // 性能极高，属于 O(1) 级别的无锁/轻量锁操作。
    bool Allocate(uint64_t length, ShmAllocation* allocation);

    // Write = Allocate + memcpy + Checksum
    // 高级封装，直接将业务数据砸进共享内存。
    bool Write(const void* data, uint64_t length, ShmAllocation* allocation);

    // 【架构核心】：线性分配器的生命周期重置
    // 将 cursor_ 强行拨回 0。这会使得下一次 Allocate 直接覆盖旧数据。
    // ⚠️ 注意：调用此方法前，必须确保 Go 端已经读取并持久化了之前所有的 Block，
    // 否则会发生极其严重的数据覆写（Data Corruption）！
    void Reset();

    const std::string& shm_name() const { return shm_name_; }
    uint64_t size() const { return size_; }
    uint64_t used() const;
    bool initialized() const;

    // 静态校验和算法（通常实现为 CRC32-IEEE）
    static uint32_t ChecksumIEEE(const void* data, uint64_t length);

   private:
    std::string shm_name_;    // 业务层的名字
    std::string posix_name_;  // 经过 NormalizeName 规范化后的 POSIX
                              // 共享内存名（必须以 '/' 开头）

    uint64_t size_;  // 预分配的总物理页大小（如 64MB）

    // 【性能核心】：线性分配游标
    // 记录当前池子分配到了哪个偏移量。每次 Allocate 都会增加它。
    uint64_t cursor_;

    int fd_;  // shm_open 返回的文件描述符

    // 【零拷贝基址】：mmap 返回的当前进程用户态起始虚拟地址
    // 所有的分配逻辑物理上就是：return base_ + cursor_
    uint8_t* base_;

    bool initialized_;

    // 保护 cursor_ 在多线程并发 Allocate/Write 时不会发生指针踩踏。
    mutable std::mutex mutex_;

    // 将普通字符串转为合法的 shm 路径（例如保证前缀有 "/"）
    bool NormalizeName(const std::string& name);
};