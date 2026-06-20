#include "cache_key.h"
#include "connector.h"

#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <iterator>
#include <limits>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#ifdef KVCACHE_USE_READLINE
#include <readline/history.h>
#include <readline/readline.h>
#endif

namespace {

constexpr uint64_t kTokensPerBlock = 16;

void PrintUsage(std::ostream &os) {
  os << "用法:\n"
     << "  kvcache_client\n"
     << "  kvcache_client interactive\n"
     << "  kvcache_client text --model-id <id> --model-revision <revision> "
        "[--text <文本> | --text-file <路径> | --stdin] [--rdma-addr <地址>]\n"
     << "  kvcache_client put --block-id <id> [--rdma-addr <地址>] "
        "[--data <文本> | --file <路径>]\n"
     << "  kvcache_client get --block-id <id> [--p2p-addr <地址>] "
        "[--out <路径> | --print]\n"
     << "  kvcache_client delete --block-id <id> [--p2p-addr <地址>]\n"
     << "\n"
     << "不带参数运行以打开简单交互界面。\n"
     << "文本 KV 会按每 16 个 token 切块，并从模型与 token 前缀自动派生 block "
        "ID。\n"
     << "\n"
     << "选项:\n"
     << "  --rdma-addr <地址>    RDMA 写入入口 "
        "(默认: 127.0.0.1:19100)\n"
     << "  --p2p-fallback-addr <地址>  P2P 降级写入入口 "
        "(默认: 127.0.0.1:19090)\n"
     << "  --p2p-addr <地址>      P2P 读取/删除入口 "
        "(默认: 127.0.0.1:19090)\n"
     << "  --no-p2p-fallback     RDMA 失败时不降级到 P2P\n"
     << "  --block-id <id>       要写入的 block ID\n"
     << "  --namespace <name>    KV Cache 隔离空间 (text 默认: default)\n"
     << "  --model-id <id>       模型标识 (text 必填)\n"
     << "  --model-revision <id> 模型权重版本 (text 必填)\n"
     << "  --adapter-id <id>     可选 LoRA/adapter 标识\n"
     << "  --text <文本>         输入文本，客户端生成 KV payload 后写入\n"
     << "  --text-file <路径>    从文本文件读取完整输入，保留换行\n"
     << "  --stdin               从标准输入读取完整输入，保留换行\n"
     << "  --data <文本>         原始内联数据负载 (仅 put 调试可用)\n"
     << "  --file <路径>         原始文件 payload (仅 put 调试可用)\n"
     << "  --out <路径>          get 时把原始 payload 写入文件\n"
     << "  --print               get 时把原始 payload 打印到 stdout\n"
     << "  --layers <数量>       模拟 KV layer 数 (默认: 1)\n"
     << "  --heads <数量>        模拟 KV head 数 (默认: 8)\n"
     << "  --head-dim <维度>     模拟每个 head 的维度 (默认: 16)\n"
     << "  --no-ack              不等待守护进程的 ACK 确认\n"
     << "  --help                显示此帮助信息\n";
}

KVCacheConnectorOptions DefaultOptions() {
  KVCacheConnectorOptions options;
  options.rdma_addr = "127.0.0.1:19100";
  options.p2p_fallback_addr = "127.0.0.1:19090";
  options.enable_p2p_fallback = true;
  options.wait_for_ack = true;
  return options;
}

bool ParseUint64(const std::string &value, uint64_t *out) {
  if (out == nullptr || value.empty()) {
    return false;
  }
  char *end = nullptr;
  errno = 0;
  unsigned long long parsed = std::strtoull(value.c_str(), &end, 10);
  if (errno != 0 || end == value.c_str() || *end != '\0') {
    return false;
  }
  if (parsed > std::numeric_limits<uint64_t>::max()) {
    return false;
  }
  *out = static_cast<uint64_t>(parsed);
  return true;
}

bool ParseUint32(const std::string &value, uint32_t *out) {
  uint64_t parsed = 0;
  if (!ParseUint64(value, &parsed) || parsed == 0 ||
      parsed > std::numeric_limits<uint32_t>::max()) {
    return false;
  }
  *out = static_cast<uint32_t>(parsed);
  return true;
}

bool ReadFile(const std::string &path, std::vector<uint8_t> *out) {
  if (out == nullptr || path.empty()) {
    return false;
  }
  std::ifstream file(path, std::ios::binary);
  if (!file) {
    std::cerr << "无法打开负载文件: " << path << "\n";
    return false;
  }
  file.seekg(0, std::ios::end);
  std::streamoff size = file.tellg();
  if (size <= 0) {
    std::cerr << "负载文件为空: " << path << "\n";
    return false;
  }
  file.seekg(0, std::ios::beg);

  out->resize(static_cast<std::size_t>(size));
  if (!file.read(reinterpret_cast<char *>(out->data()), size)) {
    std::cerr << "无法读取负载文件: " << path << "\n";
    return false;
  }
  return true;
}

bool WriteFile(const std::string &path, const std::vector<uint8_t> &data) {
  if (path.empty()) {
    return false;
  }
  std::ofstream file(path, std::ios::binary | std::ios::trunc);
  if (!file) {
    std::cerr << "无法打开输出文件: " << path << "\n";
    return false;
  }
  if (!data.empty()) {
    file.write(reinterpret_cast<const char *>(data.data()),
               static_cast<std::streamsize>(data.size()));
  }
  if (!file) {
    std::cerr << "无法写入输出文件: " << path << "\n";
    return false;
  }
  return true;
}

std::vector<uint8_t> BytesFromString(const std::string &value) {
  return std::vector<uint8_t>(value.begin(), value.end());
}

std::string HexPreview(const std::vector<uint8_t> &data,
                       std::size_t max_bytes = 64) {
  std::ostringstream os;
  const std::size_t n = std::min(max_bytes, data.size());
  for (std::size_t i = 0; i < n; ++i) {
    if (i != 0) {
      os << ' ';
    }
    os << std::hex << std::setw(2) << std::setfill('0')
       << static_cast<unsigned>(data[i]);
  }
  if (data.size() > n) {
    os << " ...";
  }
  return os.str();
}

struct TextKVShape {
  uint32_t layers = 1;
  uint32_t heads = 8;
  uint32_t head_dim = 16;
};

void SetTextKVError(std::string *error_message, const std::string &message) {
  if (error_message != nullptr) {
    *error_message = message;
  }
}

std::vector<uint32_t> TokenizeUtf8Bytes(const std::string &text) {
  std::vector<uint32_t> tokens;
  tokens.reserve(text.size());
  for (unsigned char ch : text) {
    tokens.push_back(static_cast<uint32_t>(ch));
  }
  return tokens;
}

bool ComputeTextKVElementCount(uint64_t token_count, const TextKVShape &shape,
                               uint64_t *elements, std::string *error_message) {
  if (shape.layers == 0 || shape.heads == 0 || shape.head_dim == 0) {
    SetTextKVError(error_message, "layers/heads/head_dim must be positive");
    return false;
  }

  uint64_t value = token_count;
  const uint64_t max = std::numeric_limits<uint64_t>::max();
  const uint64_t factors[] = {shape.layers, 2, shape.heads, shape.head_dim};
  for (uint64_t factor : factors) {
    if (value > max / factor) {
      SetTextKVError(error_message, "KV element count overflow");
      return false;
    }
    value *= factor;
  }

  if (value == 0 ||
      value > std::numeric_limits<std::size_t>::max() / sizeof(float)) {
    SetTextKVError(error_message, "KV payload is too large");
    return false;
  }
  *elements = value;
  return true;
}

float BuildTextKVValue(uint32_t token, uint64_t token_idx, uint64_t layer_idx,
                       uint32_t kv_kind, uint32_t head, uint32_t dim) {
  uint32_t mixed = token * 1664525u;
  mixed += static_cast<uint32_t>(token_idx + 1) * 1013904223u;
  mixed += static_cast<uint32_t>(layer_idx + 1) * 2654435761u;
  mixed += head * 2246822519u;
  mixed += dim * 3266489917u;
  mixed += kv_kind * 668265263u;

  float value = static_cast<float>(mixed & 0xffffu) / 65535.0f;
  return kv_kind == 0 ? value : -value;
}

bool BuildTextKVChunkOnCPU(const std::vector<uint32_t> &tokens,
                           uint64_t token_begin, uint64_t token_end,
                           const TextKVShape &shape,
                           std::vector<uint8_t> *payload,
                           std::string *error_message) {
  if (error_message != nullptr) {
    error_message->clear();
  }
  if (payload == nullptr) {
    SetTextKVError(error_message, "nil KV payload output");
    return false;
  }
  payload->clear();
  if (token_begin >= token_end || token_end > tokens.size()) {
    SetTextKVError(error_message, "invalid token range");
    return false;
  }

  const uint64_t token_count = token_end - token_begin;
  uint64_t elements = 0;
  if (!ComputeTextKVElementCount(token_count, shape, &elements,
                                 error_message)) {
    return false;
  }

  std::vector<float> kv(elements);
  uint64_t index = 0;
  for (uint64_t layer = 0; layer < shape.layers; ++layer) {
    for (uint64_t token = token_begin; token < token_end; ++token) {
      for (uint32_t kv_kind = 0; kv_kind < 2; ++kv_kind) {
        for (uint32_t head = 0; head < shape.heads; ++head) {
          for (uint32_t dim = 0; dim < shape.head_dim; ++dim) {
            kv[index++] = BuildTextKVValue(tokens[token], token, layer, kv_kind,
                                           head, dim);
          }
        }
      }
    }
  }
  if (index != elements) {
    SetTextKVError(error_message, "generated KV element count mismatch");
    return false;
  }

  payload->resize(kv.size() * sizeof(float));
  std::memcpy(payload->data(), kv.data(), payload->size());
  return true;
}

struct PutCommand {
  KVCacheConnectorOptions options;
  uint64_t block_id = 0;
  bool has_block_id = false;
  bool has_data = false;
  bool has_file = false;
  std::string data;
  std::string file;
};

struct TextCommand {
  KVCacheConnectorOptions options;
  TextKVShape shape;
  KVCacheScope cache_scope;
  std::string text;
  bool has_text = false;
  std::string text_file;
  bool has_text_file = false;
  bool read_stdin = false;
};

struct GetCommand {
  KVCacheConnectorOptions options;
  uint64_t block_id = 0;
  bool has_block_id = false;
  std::string output_path;
  bool has_output_path = false;
  bool print_payload = false;
};

struct DeleteCommand {
  KVCacheConnectorOptions options;
  uint64_t block_id = 0;
  bool has_block_id = false;
};

bool NeedValue(int index, int argc, const char *flag) {
  if (index + 1 < argc) {
    return true;
  }
  std::cerr << "缺少参数值: " << flag << "\n";
  return false;
}

bool ParsePutCommand(int argc, char **argv, PutCommand *cmd) {
  if (cmd == nullptr) {
    return false;
  }
  cmd->options = DefaultOptions();

  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(std::cout);
      std::exit(0);
    }
    if (arg == "--rdma-addr") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->options.rdma_addr = argv[++i];
      continue;
    }
    if (arg == "--block-id") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      if (!ParseUint64(argv[++i], &cmd->block_id) || cmd->block_id == 0) {
        std::cerr << "无效的 --block-id\n";
        return false;
      }
      cmd->has_block_id = true;
      continue;
    }
    if (arg == "--data") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->data = argv[++i];
      cmd->has_data = true;
      continue;
    }
    if (arg == "--file") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->file = argv[++i];
      cmd->has_file = true;
      continue;
    }
    if (arg == "--p2p-fallback-addr" || arg == "--p2p-addr") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->options.p2p_fallback_addr = argv[++i];
      continue;
    }
    if (arg == "--no-p2p-fallback") {
      cmd->options.enable_p2p_fallback = false;
      continue;
    }
    if (arg == "--no-ack") {
      cmd->options.wait_for_ack = false;
      continue;
    }
    std::cerr << "未知选项: " << arg << "\n";
    return false;
  }

  if (!cmd->has_block_id) {
    std::cerr << "必须提供 --block-id\n";
    return false;
  }
  if (cmd->has_data == cmd->has_file) {
    std::cerr << "必须且只能提供 --data 或 --file 中的一个\n";
    return false;
  }
  return true;
}

bool ParseCommonKVOptions(const std::string &arg, int *index, int argc,
                          char **argv, KVCacheConnectorOptions *options,
                          TextKVShape *shape) {
  if (index == nullptr || options == nullptr || shape == nullptr) {
    return false;
  }
  int i = *index;
  if (arg == "--rdma-addr") {
    if (!NeedValue(i, argc, arg.c_str()))
      return false;
    options->rdma_addr = argv[++i];
  } else if (arg == "--p2p-fallback-addr" || arg == "--p2p-addr") {
    if (!NeedValue(i, argc, arg.c_str()))
      return false;
    options->p2p_fallback_addr = argv[++i];
  } else if (arg == "--no-p2p-fallback") {
    options->enable_p2p_fallback = false;
  } else if (arg == "--no-ack") {
    options->wait_for_ack = false;
  } else if (arg == "--layers") {
    if (!NeedValue(i, argc, arg.c_str()) ||
        !ParseUint32(argv[++i], &shape->layers)) {
      std::cerr << "无效的 --layers\n";
      return false;
    }
  } else if (arg == "--heads") {
    if (!NeedValue(i, argc, arg.c_str()) ||
        !ParseUint32(argv[++i], &shape->heads)) {
      std::cerr << "无效的 --heads\n";
      return false;
    }
  } else if (arg == "--head-dim") {
    if (!NeedValue(i, argc, arg.c_str()) ||
        !ParseUint32(argv[++i], &shape->head_dim)) {
      std::cerr << "无效的 --head-dim\n";
      return false;
    }
  } else {
    return false;
  }
  *index = i;
  return true;
}

bool ParseTextCommand(int argc, char **argv, TextCommand *cmd) {
  if (cmd == nullptr) {
    return false;
  }
  cmd->options = DefaultOptions();

  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(std::cout);
      std::exit(0);
    }
    if (arg == "--block-id") {
      std::cerr << "text 命令会从 KVCacheKey 自动生成 block ID；"
                   "--block-id 仅用于 put/get/delete 调试命令\n";
      return false;
    }
    if (arg == "--namespace") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->cache_scope.cache_namespace = argv[++i];
      continue;
    }
    if (arg == "--model-id") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->cache_scope.model_id = argv[++i];
      continue;
    }
    if (arg == "--model-revision") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->cache_scope.model_revision = argv[++i];
      continue;
    }
    if (arg == "--adapter-id") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->cache_scope.adapter_id = argv[++i];
      continue;
    }
    if (arg == "--text") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->text = argv[++i];
      cmd->has_text = true;
      continue;
    }
    if (arg == "--text-file") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->text_file = argv[++i];
      cmd->has_text_file = true;
      continue;
    }
    if (arg == "--stdin") {
      cmd->read_stdin = true;
      continue;
    }
    if (ParseCommonKVOptions(arg, &i, argc, argv, &cmd->options, &cmd->shape)) {
      continue;
    }
    std::cerr << "未知选项: " << arg << "\n";
    return false;
  }

  if (cmd->cache_scope.model_id.empty()) {
    std::cerr << "text 命令必须提供 --model-id\n";
    return false;
  }
  if (cmd->cache_scope.model_revision.empty()) {
    std::cerr << "text 命令必须提供 --model-revision\n";
    return false;
  }
  int text_source_count = 0;
  if (cmd->has_text)
    ++text_source_count;
  if (cmd->has_text_file)
    ++text_source_count;
  if (cmd->read_stdin)
    ++text_source_count;
  if (text_source_count != 1) {
    std::cerr << "必须且只能提供 --text、--text-file、--stdin 中的一个\n";
    return false;
  }
  return true;
}

bool ParseGetCommand(int argc, char **argv, GetCommand *cmd) {
  if (cmd == nullptr) {
    return false;
  }
  cmd->options = DefaultOptions();

  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(std::cout);
      std::exit(0);
    }
    if (arg == "--block-id") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      if (!ParseUint64(argv[++i], &cmd->block_id) || cmd->block_id == 0) {
        std::cerr << "无效的 --block-id\n";
        return false;
      }
      cmd->has_block_id = true;
      continue;
    }
    if (arg == "--p2p-addr" || arg == "--p2p-fallback-addr") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->options.p2p_fallback_addr = argv[++i];
      continue;
    }
    if (arg == "--out") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->output_path = argv[++i];
      cmd->has_output_path = true;
      continue;
    }
    if (arg == "--print") {
      cmd->print_payload = true;
      continue;
    }
    std::cerr << "未知选项: " << arg << "\n";
    return false;
  }

  if (!cmd->has_block_id) {
    std::cerr << "必须提供 --block-id\n";
    return false;
  }
  if (cmd->has_output_path && cmd->print_payload) {
    std::cerr << "不能同时提供 --out 和 --print\n";
    return false;
  }
  return true;
}

bool ParseDeleteCommand(int argc, char **argv, DeleteCommand *cmd) {
  if (cmd == nullptr) {
    return false;
  }
  cmd->options = DefaultOptions();

  for (int i = 2; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg == "--help") {
      PrintUsage(std::cout);
      std::exit(0);
    }
    if (arg == "--block-id") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      if (!ParseUint64(argv[++i], &cmd->block_id) || cmd->block_id == 0) {
        std::cerr << "无效的 --block-id\n";
        return false;
      }
      cmd->has_block_id = true;
      continue;
    }
    if (arg == "--p2p-addr" || arg == "--p2p-fallback-addr") {
      if (!NeedValue(i, argc, arg.c_str()))
        return false;
      cmd->options.p2p_fallback_addr = argv[++i];
      continue;
    }
    std::cerr << "未知选项: " << arg << "\n";
    return false;
  }

  if (!cmd->has_block_id) {
    std::cerr << "必须提供 --block-id\n";
    return false;
  }
  return true;
}

int PublishPayload(const KVCacheConnectorOptions &options, uint64_t block_id,
                   const std::vector<uint8_t> &payload) {
  if (payload.empty()) {
    std::cerr << "数据负载不能为空\n";
    return 1;
  }

  KVCacheConnector connector(options);
  if (!connector.Connect()) {
    std::cerr << "无法连接到 kvcache 守护进程: rdma_addr=" << options.rdma_addr
              << " p2p_fallback_addr=" << options.p2p_fallback_addr << "\n";
    return 1;
  }

  KVCacheBlockMeta meta;
  if (!connector.PutBlock(block_id, payload.data(), payload.size(), &meta)) {
    std::cerr << "无法发布块 " << block_id << "\n";
    return 1;
  }

  std::cout << "已发布块" << " seq=" << meta.seq
            << " block_id=" << meta.block_id << " transport=" << meta.transport
            << " length=" << meta.length << " checksum=" << meta.checksum
            << "\n";
  return 0;
}

int RunPut(const PutCommand &cmd) {
  std::vector<uint8_t> payload;
  if (cmd.has_file) {
    if (!ReadFile(cmd.file, &payload))
      return 1;
  } else {
    payload = BytesFromString(cmd.data);
  }
  return PublishPayload(cmd.options, cmd.block_id, payload);
}

int RunGet(const GetCommand &cmd) {
  KVCacheConnector connector(cmd.options);
  if (!connector.Connect()) {
    std::cerr << "无法连接到 kvcache P2P 入口: "
              << cmd.options.p2p_fallback_addr << "\n";
    return 1;
  }

  std::vector<uint8_t> payload;
  KVCacheBlockMeta meta;
  if (!connector.GetBlock(cmd.block_id, &payload, &meta)) {
    std::cerr << "无法读取块 " << cmd.block_id << "\n";
    return 1;
  }

  if (cmd.has_output_path) {
    if (!WriteFile(cmd.output_path, payload)) {
      return 1;
    }
    std::cout << "已读取块" << " block_id=" << meta.block_id
              << " length=" << meta.length << " checksum=" << meta.checksum
              << " out=" << cmd.output_path << "\n";
    return 0;
  }
  if (cmd.print_payload) {
    if (!payload.empty()) {
      std::cout.write(reinterpret_cast<const char *>(payload.data()),
                      static_cast<std::streamsize>(payload.size()));
    }
    return std::cout ? 0 : 1;
  }

  std::cout << "已读取块" << " block_id=" << meta.block_id
            << " length=" << meta.length << " checksum=" << meta.checksum
            << " preview_hex=\"" << HexPreview(payload) << "\"\n";
  return 0;
}

int RunDelete(const DeleteCommand &cmd) {
  KVCacheConnector connector(cmd.options);
  if (!connector.Connect()) {
    std::cerr << "无法连接到 kvcache P2P 入口: "
              << cmd.options.p2p_fallback_addr << "\n";
    return 1;
  }
  if (!connector.DeleteBlock(cmd.block_id)) {
    std::cerr << "无法删除块 " << cmd.block_id << "\n";
    return 1;
  }
  std::cout << "已删除本节点块 block_id=" << cmd.block_id << "\n";
  return 0;
}

int PublishTextKV(const KVCacheConnectorOptions &options,
                  const TextKVShape &shape, const KVCacheScope &base_scope,
                  const std::string &text) {
  if (text.empty()) {
    std::cerr << "输入文本不能为空\n";
    return 1;
  }

  const std::vector<uint32_t> tokens = TokenizeUtf8Bytes(text);
  if (tokens.empty()) {
    std::cerr << "tokenizer 没有生成 token\n";
    return 1;
  }

  KVCacheScope scope = base_scope;
  scope.chunk_size = static_cast<uint32_t>(kTokensPerBlock);
  scope.layout.version = 1;
  scope.layout.dtype = "fp32";
  scope.layout.layers = shape.layers;
  scope.layout.heads = shape.heads;
  scope.layout.head_dim = shape.head_dim;
  scope.layout.tp_world_size = 1;
  scope.layout.tp_rank = 0;

  std::vector<KVCacheChunkKey> keys;
  std::string error_message;
  if (!BuildKVCacheKeys(scope, tokens, &keys, &error_message)) {
    std::cerr << "生成 KVCacheKey 失败: " << error_message << "\n";
    return 1;
  }

  uint64_t total_elements = 0;
  if (!ComputeTextKVElementCount(tokens.size(), shape, &total_elements,
                                 &error_message)) {
    std::cerr << "计算 KV payload 尺寸失败: " << error_message << "\n";
    return 1;
  }
  const uint64_t token_count = tokens.size();
  const uint64_t total_kv_bytes = total_elements * sizeof(float);

  KVCacheConnector connector(options);
  if (!connector.Connect()) {
    std::cerr << "无法连接到 kvcache 守护进程: rdma_addr=" << options.rdma_addr
              << " p2p_fallback_addr=" << options.p2p_fallback_addr << "\n";
    return 1;
  }

  std::cout << "准备查询/发布 KV Cache"
            << " namespace=" << scope.cache_namespace
            << " model=" << scope.model_id
            << " revision=" << scope.model_revision << " adapter="
            << (scope.adapter_id.empty() ? "none" : scope.adapter_id)
            << " tokens=" << token_count
            << " tokens_per_block=" << kTokensPerBlock
            << " blocks=" << keys.size() << " total_kv_bytes=" << total_kv_bytes
            << " shape=[layers=" << shape.layers
            << ", kv=2, heads=" << shape.heads
            << ", head_dim=" << shape.head_dim << "]\n";

  std::vector<KVCacheChunkKey> cacheable_keys;
  for (const auto &key : keys) {
    if (key.token_end - key.token_begin == scope.chunk_size) {
      cacheable_keys.push_back(key);
    }
  }

  std::size_t hit_blocks = 0;
  uint64_t matched_tokens = 0;
  KVCachePrefixLookup prefix_lookup;
  KVCacheLookupResult lookup_status = KVCacheLookupResult::kNotFound;
  if (!cacheable_keys.empty()) {
    lookup_status =
        connector.LookupPrefix(cacheable_keys.front().key.scope_digest,
                               cacheable_keys, &prefix_lookup);
  }
  if (lookup_status == KVCacheLookupResult::kError) {
    std::cerr << "Prefix Lookup 不可用，本次安全降级为重新计算。\n";
  }
  if (lookup_status == KVCacheLookupResult::kFound) {
    for (std::size_t index = 0; index < prefix_lookup.entries.size(); ++index) {
      const KVCacheChunkKey &chunk = cacheable_keys[index];
      const KVCachePrefixLocation &location = prefix_lookup.entries[index];
      std::vector<uint8_t> cached_payload;
      KVCacheBlockMeta cached_meta;
      if (!connector.LoadPrefixEntry(location, &cached_payload, &cached_meta)) {
        std::cerr << "加载命中 KV Cache 失败，从 block_id=" << chunk.block_id
                  << " 开始重新计算。\n";
        break;
      }
      uint64_t expected_elements = 0;
      if (!ComputeTextKVElementCount(chunk.token_end - chunk.token_begin, shape,
                                     &expected_elements, &error_message)) {
        std::cerr << "计算命中 KV block 尺寸失败: " << error_message << "\n";
        return 1;
      }
      const uint64_t expected_bytes = expected_elements * sizeof(float);
      if (cached_payload.size() != expected_bytes) {
        std::cerr << "KVCacheKey 命中的 payload 布局不匹配 block_id="
                  << chunk.block_id << " expected_bytes=" << expected_bytes
                  << " actual_bytes=" << cached_payload.size() << "\n";
        return 1;
      }
      matched_tokens = chunk.token_end;
      ++hit_blocks;
      std::cout << "命中 KV block" << " block_id=" << chunk.block_id
                << " object_id=" << KVCacheDigestHex(chunk.object_id)
                << " token_range=[" << chunk.token_begin << ", "
                << chunk.token_end << ")" << " source_node=" << location.node_id
                << " source_addr=" << location.address
                << " kv_bytes=" << cached_payload.size() << "\n";
    }
  }
  if (prefix_lookup.lease_id != 0 &&
      !connector.ReleasePrefixLease(prefix_lookup.lease_id)) {
    std::cerr << "释放 Prefix Lookup 租约失败；daemon 将在 TTL 后自动释放。\n";
  }

  uint64_t written = 0;
  for (std::size_t block_index = hit_blocks; block_index < keys.size();
       ++block_index) {
    const KVCacheChunkKey &chunk = keys[block_index];
    std::vector<uint8_t> block_payload;
    if (!BuildTextKVChunkOnCPU(tokens, chunk.token_begin, chunk.token_end,
                               shape, &block_payload, &error_message)) {
      std::cerr << "生成 KV block 失败: " << error_message << "\n";
      return 1;
    }

    const bool complete_block =
        chunk.token_end - chunk.token_begin == scope.chunk_size;
    if (!complete_block) {
      std::cout << "已计算尾部 KV（不足完整 block，不进入缓存）"
                << " token_range=[" << chunk.token_begin << ", "
                << chunk.token_end << ")"
                << " kv_bytes=" << block_payload.size() << "\n";
      continue;
    }

    KVCacheBlockMeta meta;
    if (!connector.PutCacheObject(chunk, block_payload.data(),
                                  block_payload.size(), &meta)) {
      std::cerr << "无法发布 KV block " << chunk.block_id << " token_range=["
                << chunk.token_begin << ", " << chunk.token_end << ")\n";
      return 1;
    }
    ++written;

    std::cout << "已发布 KV block" << " seq=" << meta.seq
              << " block_id=" << meta.block_id
              << " object_id=" << KVCacheDigestHex(chunk.object_id)
              << " token_range=[" << chunk.token_begin << ", "
              << chunk.token_end << ")"
              << " tokens=" << (chunk.token_end - chunk.token_begin)
              << " kv_bytes=" << block_payload.size()
              << " transport=" << meta.transport
              << " checksum=" << meta.checksum << "\n";
  }
  std::cout << "KV Cache 结果" << " matched_tokens=" << matched_tokens
            << " computed_tokens=" << (token_count - matched_tokens)
            << " hit_blocks=" << hit_blocks << " written_blocks=" << written
            << "\n";
  return 0;
}

int RunText(const TextCommand &cmd) {
  std::string text = cmd.text;
  if (cmd.has_text_file) {
    std::vector<uint8_t> bytes;
    if (!ReadFile(cmd.text_file, &bytes))
      return 1;
    text.assign(bytes.begin(), bytes.end());
  } else if (cmd.read_stdin) {
    text.assign(std::istreambuf_iterator<char>(std::cin),
                std::istreambuf_iterator<char>());
  }
  return PublishTextKV(cmd.options, cmd.shape, cmd.cache_scope, text);
}

bool ReadLine(const std::string &prompt, std::string *out) {
  if (out == nullptr)
    return false;
#ifdef KVCACHE_USE_READLINE
  char *line = readline(prompt.c_str());
  if (line == nullptr) {
    std::cout << "\n";
    return false;
  }
  *out = line;
  if (!out->empty()) {
    add_history(line);
  }
  std::free(line);
  return true;
#else
  std::cout << prompt;
  if (!std::getline(std::cin, *out)) {
    std::cout << "\n";
    return false;
  }
  return true;
#endif
}

bool ReadMultilineText(std::string *out) {
  if (out == nullptr)
    return false;
  out->clear();

  std::cout << "输入文本，支持多行；空行提交，:cancel 取消。\n";
  for (;;) {
    std::string line;
    if (!ReadLine("text> ", &line)) {
      return !out->empty();
    }
    if (line == ":cancel") {
      out->clear();
      return false;
    }
    if (line.empty()) {
      return true;
    }
    if (!out->empty()) {
      out->push_back('\n');
    }
    out->append(line);
  }
}

bool PromptUint64(const std::string &prompt, uint64_t *out) {
  for (;;) {
    std::string line;
    if (!ReadLine(prompt, &line))
      return false;
    if (ParseUint64(line, out) && *out != 0)
      return true;
    std::cout << "请输入一个正整数。\n";
  }
}

void ShowConfig(const KVCacheConnectorOptions &options) {
  std::cout << "\n当前配置\n"
            << "  rdma_addr=" << options.rdma_addr << "\n"
            << "  p2p_fallback_addr=" << options.p2p_fallback_addr << "\n"
            << "  enable_p2p_fallback="
            << (options.enable_p2p_fallback ? "true" : "false") << "\n"
            << "  wait_for_ack=" << (options.wait_for_ack ? "true" : "false")
            << "\n\n";
}

void ShowKVShape(const TextKVShape &shape, const KVCacheScope &scope) {
  std::cout << "  namespace=" << scope.cache_namespace << "\n"
            << "  model_id=" << scope.model_id << "\n"
            << "  model_revision=" << scope.model_revision << "\n"
            << "  adapter_id="
            << (scope.adapter_id.empty() ? "none" : scope.adapter_id) << "\n"
            << "  kv_shape=[layers=" << shape.layers
            << ", kv=2, heads=" << shape.heads
            << ", head_dim=" << shape.head_dim << "]\n\n";
}

bool ConfigureClient(KVCacheConnectorOptions *options, TextKVShape *shape,
                     KVCacheScope *scope) {
  if (options == nullptr)
    return false;
  std::string line;
  std::cout << "\n按回车键保持当前值。\n";

  if (!ReadLine("RDMA 写入地址 (rdma addr) [" + options->rdma_addr + "]: ",
                &line))
    return false;
  if (!line.empty())
    options->rdma_addr = line;

  if (!ReadLine("P2P 降级地址 (p2p fallback addr) [" +
                    options->p2p_fallback_addr + "]: ",
                &line))
    return false;
  if (!line.empty())
    options->p2p_fallback_addr = line;

  if (!ReadLine(
          "启用 P2P 降级 (true/false) [" +
              std::string(options->enable_p2p_fallback ? "true" : "false") +
              "]: ",
          &line))
    return false;
  if (line == "true" || line == "1" || line == "yes" || line == "是") {
    options->enable_p2p_fallback = true;
  } else if (line == "false" || line == "0" || line == "no" || line == "否") {
    options->enable_p2p_fallback = false;
  } else if (!line.empty()) {
    std::cout << "无效的 P2P 降级配置，保持原值。\n";
  }

  if (scope != nullptr) {
    if (!ReadLine("KV namespace [" + scope->cache_namespace + "]: ", &line))
      return false;
    if (!line.empty())
      scope->cache_namespace = line;

    if (!ReadLine("模型 ID (model id) [" + scope->model_id + "]: ", &line))
      return false;
    if (!line.empty())
      scope->model_id = line;

    if (!ReadLine("模型版本 (model revision) [" + scope->model_revision + "]: ",
                  &line))
      return false;
    if (!line.empty())
      scope->model_revision = line;

    if (!ReadLine("Adapter ID，留空表示无 [" + scope->adapter_id + "]: ",
                  &line))
      return false;
    scope->adapter_id = line;
  }

  if (shape != nullptr) {
    uint32_t parsed = 0;
    if (!ReadLine("KV layers [" + std::to_string(shape->layers) + "]: ", &line))
      return false;
    if (!line.empty() && ParseUint32(line, &parsed)) {
      shape->layers = parsed;
    } else if (!line.empty()) {
      std::cout << "无效的 layers，保持原值。\n";
    }

    if (!ReadLine("KV heads [" + std::to_string(shape->heads) + "]: ", &line))
      return false;
    if (!line.empty() && ParseUint32(line, &parsed)) {
      shape->heads = parsed;
    } else if (!line.empty()) {
      std::cout << "无效的 heads，保持原值。\n";
    }

    if (!ReadLine("KV head_dim [" + std::to_string(shape->head_dim) + "]: ",
                  &line))
      return false;
    if (!line.empty() && ParseUint32(line, &parsed)) {
      shape->head_dim = parsed;
    } else if (!line.empty()) {
      std::cout << "无效的 head_dim，保持原值。\n";
    }
  }
  return true;
}

int PutTextKVInteractive(const KVCacheConnectorOptions &options,
                         const TextKVShape &shape, const KVCacheScope &scope) {
  std::string data;
  if (!ReadMultilineText(&data))
    return 1;
  return PublishTextKV(options, shape, scope, data);
}

int PutInlineInteractive(const KVCacheConnectorOptions &options) {
  uint64_t block_id = 0;
  if (!PromptUint64("块 ID (block id): ", &block_id))
    return 1;
  std::string data;
  if (!ReadLine("负载文本 (payload text): ", &data))
    return 1;
  return PublishPayload(options, block_id, BytesFromString(data));
}

int PutFileInteractive(const KVCacheConnectorOptions &options) {
  uint64_t block_id = 0;
  if (!PromptUint64("块 ID (block id): ", &block_id))
    return 1;
  std::string path;
  if (!ReadLine("负载文件路径 (payload file path): ", &path))
    return 1;
  std::vector<uint8_t> payload;
  if (!ReadFile(path, &payload))
    return 1;
  return PublishPayload(options, block_id, payload);
}

int GetInteractive(const KVCacheConnectorOptions &options) {
  uint64_t block_id = 0;
  if (!PromptUint64("块 ID (block id): ", &block_id))
    return 1;
  std::string path;
  if (!ReadLine("输出文件路径，留空只显示预览: ", &path))
    return 1;

  GetCommand cmd;
  cmd.options = options;
  cmd.block_id = block_id;
  cmd.has_block_id = true;
  if (!path.empty()) {
    cmd.output_path = path;
    cmd.has_output_path = true;
  }
  return RunGet(cmd);
}

int DeleteInteractive(const KVCacheConnectorOptions &options) {
  uint64_t block_id = 0;
  if (!PromptUint64("要删除的块 ID (block id): ", &block_id))
    return 1;
  std::string confirm;
  if (!ReadLine("确认删除当前 daemon 的本地副本? 输入 yes 确认: ", &confirm)) {
    return 1;
  }
  if (confirm != "yes" && confirm != "YES" && confirm != "是") {
    std::cout << "已取消删除。\n";
    return 0;
  }

  DeleteCommand cmd;
  cmd.options = options;
  cmd.block_id = block_id;
  cmd.has_block_id = true;
  return RunDelete(cmd);
}

int RunInteractive() {
  KVCacheConnectorOptions options = DefaultOptions();
  TextKVShape shape;
  KVCacheScope cache_scope;
  cache_scope.model_id = "zerokv-demo";
  cache_scope.model_revision = "v1";
  cache_scope.chunk_size = static_cast<uint32_t>(kTokensPerBlock);
  std::cout << "kvcache 简单测试客户端\n";
  ShowConfig(options);
  ShowKVShape(shape, cache_scope);

  for (;;) {
    std::cout << "请选择操作:\n"
              << "  1. 输入文本 -> 生成 KV -> 写入 kvcache\n"
              << "  2. 调试: 原始文本 payload 写入\n"
              << "  3. 调试: 文件 payload 写入\n"
              << "  4. 读取 block\n"
              << "  5. 删除当前 daemon 的本地 block 副本\n"
              << "  6. 查看/配置\n"
              << "  7. 退出\n";
    std::string choice;
    if (!ReadLine("> ", &choice))
      return 0;

    if (choice == "1") {
      (void)PutTextKVInteractive(options, shape, cache_scope);
    } else if (choice == "2") {
      (void)PutInlineInteractive(options);
    } else if (choice == "3") {
      (void)PutFileInteractive(options);
    } else if (choice == "4") {
      (void)GetInteractive(options);
    } else if (choice == "5") {
      (void)DeleteInteractive(options);
    } else if (choice == "6") {
      ShowConfig(options);
      ShowKVShape(shape, cache_scope);
      if (!ConfigureClient(&options, &shape, &cache_scope))
        return 1;
    } else if (choice == "7") {
      return 0;
    } else if (choice == "q" || choice == "quit" || choice == "退出") {
      return 0;
    } else {
      std::cout << "未知的选择。\n";
    }
  }
}

} // namespace

int main(int argc, char **argv) {
  if (argc < 2) {
    return RunInteractive();
  }
  if (std::strcmp(argv[1], "--help") == 0) {
    PrintUsage(std::cout);
    return 0;
  }

  std::string command = argv[1];
  if (command == "interactive") {
    return RunInteractive();
  }
  if (command == "put") {
    PutCommand cmd;
    if (!ParsePutCommand(argc, argv, &cmd)) {
      PrintUsage(std::cerr);
      return 1;
    }
    return RunPut(cmd);
  }
  if (command == "text") {
    TextCommand cmd;
    if (!ParseTextCommand(argc, argv, &cmd)) {
      PrintUsage(std::cerr);
      return 1;
    }
    return RunText(cmd);
  }
  if (command == "get") {
    GetCommand cmd;
    if (!ParseGetCommand(argc, argv, &cmd)) {
      PrintUsage(std::cerr);
      return 1;
    }
    return RunGet(cmd);
  }
  if (command == "delete") {
    DeleteCommand cmd;
    if (!ParseDeleteCommand(argc, argv, &cmd)) {
      PrintUsage(std::cerr);
      return 1;
    }
    return RunDelete(cmd);
  }
  std::cerr << "未知命令: " << command << "\n";
  PrintUsage(std::cerr);
  return 1;
}
