"""
测试运行脚本
运行所有单元测试并生成报告
"""

import os
import sys
import time
import unittest

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def run_all_tests():
    """运行所有测试"""
    print("=" * 60)
    print("QuackView 项目单元测试")
    print("=" * 60)

    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 添加所有测试模块
    test_modules = [
        "tests.test_utils",
        "tests.test_sql_generator",
        "tests.test_sql_executor",
        "tests.test_integration",
        "tests.test_edge_cases",
    ]

    for module_name in test_modules:
        try:
            module = __import__(module_name, fromlist=[""])
            suite.addTests(loader.loadTestsFromModule(module))
            print(f"✓ 已加载测试模块: {module_name}")
        except ImportError as e:
            print(f"✗ 无法加载测试模块 {module_name}: {e}")

    # 运行测试
    print("\n开始运行测试...")
    print("-" * 60)

    start_time = time.time()

    # 创建测试运行器
    runner = unittest.TextTestRunner(
        verbosity=2, stream=sys.stdout, descriptions=True, failfast=False
    )

    # 运行测试
    result = runner.run(suite)

    end_time = time.time()
    duration = end_time - start_time

    # 打印测试结果摘要
    print("\n" + "=" * 60)
    print("测试结果摘要")
    print("=" * 60)
    print(f"运行时间: {duration:.2f} 秒")
    print(f"测试用例总数: {result.testsRun}")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")

    # 打印失败的测试
    if result.failures:
        print("\n失败的测试:")
        print("-" * 40)
        for test, traceback in result.failures:
            print(f"✗ {test}")
            print(f"  错误: {traceback.split('AssertionError:')[-1].strip()}")
            print()

    # 打印错误的测试
    if result.errors:
        print("\n错误的测试:")
        print("-" * 40)
        for test, traceback in result.errors:
            print(f"✗ {test}")
            print(f"  错误: {traceback.split('Exception:')[-1].strip()}")
            print()

    # 返回测试结果
    return result.wasSuccessful()


def run_specific_test(test_name):
    """运行特定的测试"""
    print(f"运行特定测试: {test_name}")

    loader = unittest.TestLoader()
    suite = unittest.TestSuite()

    # 尝试加载特定测试
    try:
        if "." in test_name:
            # 格式: module.TestCase.test_method
            parts = test_name.split(".")
            if len(parts) >= 3:
                module_name = ".".join(parts[:-2])
                class_name = parts[-2]
                method_name = parts[-1]

                module = __import__(module_name, fromlist=[""])
                test_class = getattr(module, class_name)
                suite.addTest(test_class(method_name))
            else:
                print(f"错误: 测试名称格式不正确: {test_name}")
                return False
        else:
            # 格式: TestCase.test_method
            parts = test_name.split(".")
            if len(parts) == 2:
                class_name, method_name = parts
                # 尝试在所有测试模块中查找
                test_modules = [
                    "tests.test_utils",
                    "tests.test_sql_generator",
                    "tests.test_sql_executor",
                    "tests.test_integration",
                    "tests.test_edge_cases",
                ]

                found = False
                for module_name in test_modules:
                    try:
                        module = __import__(module_name, fromlist=[""])
                        test_class = getattr(module, class_name, None)
                        if test_class:
                            suite.addTest(test_class(method_name))
                            found = True
                            break
                    except (ImportError, AttributeError):
                        continue

                if not found:
                    print(f"错误: 未找到测试: {test_name}")
                    return False
            else:
                print(f"错误: 测试名称格式不正确: {test_name}")
                return False
    except Exception as e:
        print(f"错误: 无法加载测试 {test_name}: {e}")
        return False

    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)

    return result.wasSuccessful()


def run_test_coverage():
    """运行测试覆盖率分析"""
    try:
        import coverage

        print("运行测试覆盖率分析...")

        # 创建覆盖率对象
        cov = coverage.Coverage()
        cov.start()

        # 运行所有测试
        success = run_all_tests()

        # 停止覆盖率收集
        cov.stop()
        cov.save()

        # 生成覆盖率报告
        print("\n生成覆盖率报告...")
        cov.report()

        # 生成HTML报告
        cov.html_report(directory="htmlcov")
        print("HTML覆盖率报告已生成到 htmlcov/ 目录")

        return success
    except ImportError:
        print("警告: 未安装coverage模块，跳过覆盖率分析")
        print("安装命令: pip install coverage")
        return run_all_tests()


def main():
    """主函数"""
    if len(sys.argv) > 1:
        command = sys.argv[1]

        if command == "--help" or command == "-h":
            print("用法:")
            print("  python run_tests.py                    # 运行所有测试")
            print("  python run_tests.py --coverage         # 运行测试并生成覆盖率报告")
            print("  python run_tests.py TestCase.test_method  # 运行特定测试")
            print("  python run_tests.py --help             # 显示帮助信息")
            return

        elif command == "--coverage":
            success = run_test_coverage()
        else:
            # 运行特定测试
            success = run_specific_test(command)
    else:
        # 运行所有测试
        success = run_all_tests()

    # 设置退出码
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
