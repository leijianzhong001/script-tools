# 这是一个示例 Python 脚本。

def promotion(promo_func):
    print("装饰器执行了")
    return promo_func

@promotion
def f1():
    print("f1")

@promotion
def f2():
    print("f2")
