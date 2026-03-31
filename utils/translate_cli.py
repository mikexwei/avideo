import sys
import time
from pathlib import Path
from ollama import Client

# 动态引入项目根目录
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.append(str(project_root))

# 配置 Ollama 服务和模型
OLLAMA_HOST = 'http://10.0.0.43:11434'  # 如果是 40 节点请自行修改
MODEL_QWEN = 'qwen2.5:32b'
MODEL_SAKURA = 'quantumcookie/Sakura-qwen2.5-v1.0:14b'
LLM_TEMPERATURE = 0.1

def translate_cli():
    print(f"🔗 正在连接 Ollama 服务: {OLLAMA_HOST}")
    try:
        client = Client(host=OLLAMA_HOST)
        client.list() # 测试连接
    except Exception as e:
        print(f"❌ 无法连接到 Ollama: {e}")
        return

    print("✅ 服务连接成功！")
    print(f"🤖 模型 1: {MODEL_QWEN}")
    print(f"🌸 模型 2: {MODEL_SAKURA}")
    print("=" * 60)

    while True:
        try:
            text = input("\n🇯🇵 请输入日文 (输入 'q' 退出): ").strip()
            if text.lower() in ['q', 'quit', 'exit']:
                print("👋 退出翻译工具。")
                break
            
            if not text:
                continue

            print("⏳ 正在请求大模型...\n")
            
            # 共用的提示词 (你也可以根据需要为不同模型设定不同的 Prompt)
            prompt = f"你是一个资深的日文到中文的本地化翻译专家。请将以下日本影视相关的文本翻译为简体中文，要求通顺自然。只输出翻译后的文本，不需要任何解释、多余的符号或拼音：\n\n{text}"

            # 1. 调用 Qwen
            try:
                t0 = time.time()
                res_qwen = client.chat(model=MODEL_QWEN, messages=[{'role': 'user', 'content': prompt}], options={'temperature': LLM_TEMPERATURE})
                text_qwen = res_qwen['message']['content'].strip()
                print(f"🤖 [{MODEL_QWEN}] ({time.time() - t0:.2f}s):\n -> {text_qwen}\n")
            except Exception as e:
                print(f"🤖 [{MODEL_QWEN}] ❌ 翻译失败: {e}\n")

            # 2. 调用 Sakura
            try:
                t0 = time.time()
                res_sakura = client.chat(model=MODEL_SAKURA, messages=[{'role': 'user', 'content': prompt}], options={'temperature': LLM_TEMPERATURE})
                text_sakura = res_sakura['message']['content'].strip()
                print(f"🌸 [{MODEL_SAKURA}] ({time.time() - t0:.2f}s):\n -> {text_sakura}")
            except Exception as e:
                print(f"🌸 [{MODEL_SAKURA}] ❌ 翻译失败: {e}")
                
            print("-" * 60)

        except KeyboardInterrupt: # 捕获 Ctrl+C
            print("\n👋 退出翻译工具。")
            break

if __name__ == "__main__":
    translate_cli()