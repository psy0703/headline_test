set OUT=E:\MyCode\Demo_Grpc\protoc_demo\src\main\java   # 输出生成的java文件根目录
set def_cli_java=(login chat)   # 包含‘login’、‘chat’元素的数组变量
set def_internal_java=(internal) # 包含‘internal’元素的数组变量

# 将cli_def目录下的login.proto和chat.proto文件生成java类
for %%A in %def_cli_java% do (
    echo generate cli protocol java code: %%A.proto
    protoc --java_out=%OUT% ./cli_def/%%A.proto
)

# 将internal_def目录下的internal.proto文件生成java类
for %%A in %def_internal_java% do (
    echo generate internal java code: %%A.proto
    protoc --java_out=%OUT% ./internal_def/%%A.proto
)

pause