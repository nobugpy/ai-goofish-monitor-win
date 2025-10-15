# src/spider_core.py
import asyncio
import json
import os
from typing import List, Dict

from src.config import STATE_FILE
from src.scraper import scrape_xianyu


async def run_spider_task(task_name: str, debug_limit: int = 0, config_path: str = "config.json"):
    """封装爬虫任务逻辑，供外部调用"""
    if not os.path.exists(STATE_FILE):
        raise FileNotFoundError(f"登录状态文件 '{STATE_FILE}' 不存在。请先运行 login.py 生成。")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件 '{config_path}' 不存在。")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            tasks_config = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        raise Exception(f"读取或解析配置文件失败: {e}")

    # 处理prompt文件
    for task in tasks_config:
        if task.get("enabled", False) and task.get("ai_prompt_base_file") and task.get("ai_prompt_criteria_file"):
            try:
                with open(task["ai_prompt_base_file"], 'r', encoding='utf-8') as f_base:
                    base_prompt = f_base.read()
                with open(task["ai_prompt_criteria_file"], 'r', encoding='utf-8') as f_criteria:
                    criteria_text = f_criteria.read()
                task['ai_prompt_text'] = base_prompt.replace("{{CRITERIA_SECTION}}", criteria_text)
            except FileNotFoundError as e:
                print(f"警告: 任务 '{task['task_name']}' 的prompt文件缺失: {e}")
                task['ai_prompt_text'] = ""
        elif task.get("enabled", False) and task.get("ai_prompt_file"):
            try:
                with open(task["ai_prompt_file"], 'r', encoding='utf-8') as f:
                    task['ai_prompt_text'] = f.read()
            except FileNotFoundError:
                print(f"警告: 任务 '{task['task_name']}' 的prompt文件未找到")
                task['ai_prompt_text'] = ""

    # 查找指定任务
    task_found = next((task for task in tasks_config if task.get('task_name') == task_name), None)
    if not task_found or not task_found.get("enabled", False):
        raise ValueError(f"任务 '{task_name}' 不存在或已被禁用")

    # 执行单个任务
    return await scrape_xianyu(task_config=task_found, debug_limit=debug_limit)
# 在 spider_v2.py 末尾添加
async def run_spider_tasks(task_names: list = None, debug_limit: int = 0, config_path: str = "config.json"):
    """
    运行指定的多个任务（若task_names为None则运行所有启用的任务）
    """
    if not os.path.exists(STATE_FILE):
        raise FileNotFoundError(f"登录状态文件 '{STATE_FILE}' 不存在。请先运行 login.py 生成。")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"配置文件 '{config_path}' 不存在。")

    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            tasks_config = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        raise Exception(f"读取或解析配置文件失败: {e}")

    # 处理prompt文件（与原main逻辑一致）
    for task in tasks_config:
        if task.get("enabled", False) and task.get("ai_prompt_base_file") and task.get("ai_prompt_criteria_file"):
            try:
                with open(task["ai_prompt_base_file"], 'r', encoding='utf-8') as f_base:
                    base_prompt = f_base.read()
                with open(task["ai_prompt_criteria_file"], 'r', encoding='utf-8') as f_criteria:
                    criteria_text = f_criteria.read()
                task['ai_prompt_text'] = base_prompt.replace("{{CRITERIA_SECTION}}", criteria_text)
            except FileNotFoundError as e:
                print(f"警告: 任务 '{task['task_name']}' 的prompt文件缺失: {e}")
                task['ai_prompt_text'] = ""
        elif task.get("enabled", False) and task.get("ai_prompt_file"):
            try:
                with open(task["ai_prompt_file"], 'r', encoding='utf-8') as f:
                    task['ai_prompt_text'] = f.read()
            except FileNotFoundError:
                print(f"警告: 任务 '{task['task_name']}' 的prompt文件未找到")
                task['ai_prompt_text'] = ""

    # 筛选需要执行的任务
    active_task_configs = []
    if task_names:
        # 按指定任务名筛选
        for task_name in task_names:
            task_found = next((t for t in tasks_config if t.get('task_name') == task_name and t.get("enabled", False)), None)
            if task_found:
                active_task_configs.append(task_found)
            else:
                print(f"任务 '{task_name}' 不存在或已禁用，跳过执行")
    else:
        # 执行所有启用的任务
        active_task_configs = [t for t in tasks_config if t.get("enabled", False)]

    if not active_task_configs:
        print("没有需要执行的任务")
        return 0

    # 并发执行任务
    coroutines = [scrape_xianyu(task_conf, debug_limit) for task_conf in active_task_configs]
    results = await asyncio.gather(*coroutines, return_exceptions=True)

    # 统计成功处理的商品总数
    total_processed = 0
    for i, result in enumerate(results):
        task_name = active_task_configs[i]['task_name']
        if isinstance(result, Exception):
            print(f"任务 '{task_name}' 执行失败: {result}")
        else:
            total_processed += result
            print(f"任务 '{task_name}' 完成，处理了 {result} 个新商品")
    return total_processed